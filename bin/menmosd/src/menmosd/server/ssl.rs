use std::convert::Infallible;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::Path as StdPath;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{fs, str::FromStr};

use antidns::{Config as DnsConfig, Server as DnsServer};

use anyhow::Result;

use axum::http::{Request, Uri};
use axum::response::{IntoResponse, Redirect};
use axum::routing::any_service;
use axum::Router;

use axum_server::tls_rustls::RustlsConfig;
use axum_server::Handle;

use futures::future::{AbortHandle, Abortable};

use hyper::service::service_fn;

use interface::{CertificateInfo, DirectoryNode};

use tokio::{
    sync::{mpsc, oneshot},
    task::{spawn, JoinHandle},
};

use x509_parser::pem::Pem;

use crate::config::{Config, HttpsParameters, LetsEncryptUrl};
use crate::server::build_router;

async fn graceful_shutdown(handle: Handle, rx: oneshot::Receiver<()>) {
    rx.await.ok();
    tracing::info!("https layer stop signal received");
    handle.graceful_shutdown(None)
}

async fn interruptible_delay(dur: Duration, stop_rx: &mut mpsc::Receiver<()>) -> bool {
    let delay = tokio::time::sleep(dur);
    tokio::pin!(delay);

    let stop_signal = stop_rx.recv();

    tokio::select! {
        _ = &mut delay => {
            false
        }
        _ = stop_signal => {
            tracing::info!("interruptible delay received stop signal");
            true
        }
    }
}

async fn join_with_timeout<E>(dur: Duration, handle: JoinHandle<Result<(), E>>) -> bool {
    let (abort_handle, abort_registration) = AbortHandle::new_pair();
    let future = Abortable::new(handle, abort_registration);

    tokio::task::spawn(async move {
        tokio::time::sleep(dur).await;
        abort_handle.abort();
    });

    match future.await {
        Ok(Ok(Ok(_))) => true,
        _ => false,
    }
}

async fn wait_for_server_stop(
    http_handle: JoinHandle<hyper::Result<()>>,
    https_handle: JoinHandle<std::io::Result<()>>,
) {
    let join_http = join_with_timeout(Duration::from_secs(5), http_handle);
    let join_https = join_with_timeout(Duration::from_secs(5), https_handle);

    let (http_ok, https_ok) = tokio::join!(join_http, join_https);

    if !http_ok {
        tracing::error!("redirect layer failed to stop in time and was killed");
    }

    if !https_ok {
        tracing::error!("https layer failed to stop in time and was killed.")
    }
}

pub async fn use_tls(
    n: Arc<dyn DirectoryNode + Send + Sync>,
    node_cfg: Arc<Config>,
    cfg: HttpsParameters,
    mut stop_rx: mpsc::Receiver<()>,
) -> Result<()> {
    let dns_server = DnsServer::start(DnsConfig {
        host_name: cfg.dns.host_name.clone(),
        root_domain: cfg.dns.root_domain.clone(),
        public_ip: cfg.dns.public_ip,
        listen: cfg.dns.listen_address,
        nb_of_concurrent_requests: cfg.dns.nb_of_concurrent_requests,
    });

    tracing::debug!("waiting for DNS server to come up");
    tokio::time::sleep(Duration::from_secs(2)).await;

    let pem_name = cfg
        .certificate_storage_path
        .join(&cfg.dns.root_domain)
        .with_extension("pem");
    let key_name = cfg
        .certificate_storage_path
        .join(&cfg.dns.root_domain)
        .with_extension("key");

    let url = if cfg.letsencrypt_url == LetsEncryptUrl::Production {
        acme_lib::DirectoryUrl::LetsEncrypt
    } else {
        acme_lib::DirectoryUrl::LetsEncryptStaging
    };
    let persist = acme_lib::persist::FilePersist::new(&cfg.certificate_storage_path);
    let dir = acme_lib::Directory::from_url(persist, url)?;

    tracing::debug!("getting letsencrypt account");
    let account = dir.account(&cfg.letsencrypt_email)?;
    tracing::debug!("account ok");

    loop {
        const TMIN: Duration = Duration::from_secs(60 * 60 * 24 * 30);

        let time_to_exp = time_to_expiration(&pem_name);
        tracing::debug!(
            "the time to expiration of {:?} is {:?}",
            pem_name,
            time_to_exp
        );

        if time_to_exp.filter(|&t| t > TMIN).is_none() {
            // TODO: Don't do a new order with every boot.
            tracing::debug!("sending an ACME order for a new certificate");
            let new_order = account.new_order(&format!("*.{}", cfg.dns.root_domain), &[])?;

            // TODO: Review this, not sure we _need_ Arc + Mutex but didn't find anything better.
            let order_sync = Arc::new(Mutex::new(new_order));
            let ord_csr = loop {
                let auths = {
                    let order = order_sync.lock().expect("poisoned mutex");
                    if let Some(ord_csr) = order.confirm_validations() {
                        break ord_csr;
                    }

                    let auths = order.authorizations()?;
                    assert_eq!(auths.len(), 1);

                    auths
                };

                // Since we have only one domain we'll have only one authorization
                let challenge = auths[0].dns_challenge();
                dns_server.set_dns_challenge(&challenge.dns_proof()).await?;

                let order_sync = order_sync.clone();
                let handle: JoinHandle<anyhow::Result<()>> =
                    tokio::task::spawn_blocking(move || {
                        let mut order = order_sync.lock().expect("poisoned mutex");
                        challenge.validate(5000)?;
                        order.refresh()?;
                        Ok(())
                    });
                handle.await??;
            };

            // Ownership is proven. Create a private/public key pair for the
            // certificate. These are provided for convenience, you can
            // provide your own keypair instead if you want.
            let pkey = acme_lib::create_p384_key();

            let order_certificate = ord_csr.finalize_pkey(pkey, 5000)?;

            let cert = order_certificate.download_and_save_cert()?;
            fs::write(&pem_name, cert.certificate())?;
            fs::write(&key_name, cert.private_key())?;
        }

        let certificate_info = CertificateInfo::from_path(&pem_name, &key_name)?;

        // Spawn redirect server.
        tracing::debug!("starting http redirect layer on port {}", cfg.http_port);
        let (tx80, rx80) = oneshot::channel();
        let http_handle = {
            let domain = cfg.dns.host_name.to_string();
            let router = Router::new().route(
                "/*path",
                any_service(service_fn(move |r: Request<_>| {
                    let domain = domain.clone();
                    async move {
                        let path = r.uri().path();
                        tracing::debug!("redirect to https://{}/{}", &domain, path);
                        let target_uri = Uri::from_str(&format!("https://{}/{}", &domain, path))
                            .expect("problem with uri");
                        Ok::<_, Infallible>(Redirect::permanent(target_uri).into_response())
                    }
                })),
            );

            let http_srv = axum::Server::bind(&([0, 0, 0, 0], cfg.http_port).into())
                .serve(router.into_make_service_with_connect_info::<SocketAddr, _>())
                .with_graceful_shutdown(async move {
                    rx80.await.ok();
                    tracing::info!("redirect layer stop signal received");
                });

            spawn(http_srv)
        };
        tracing::debug!("redirect layer started");

        tracing::debug!("starting https layer on port {}", cfg.https_port);
        let (tx, rx) = oneshot::channel();
        let https_handle = {
            let key_name = key_name.clone();
            let pem_name = pem_name.clone();

            let router = build_router(
                node_cfg.clone(),
                n.clone(),
                Arc::from(Some(certificate_info)),
            );

            let config = RustlsConfig::from_pem_file(pem_name, key_name)
                .await
                .unwrap();

            let interrupt_handle = Handle::new();
            tokio::spawn(graceful_shutdown(interrupt_handle.clone(), rx));

            let https_srv = axum_server::bind_rustls(([0, 0, 0, 0], cfg.https_port).into(), config)
                .handle(interrupt_handle)
                .serve(router.into_make_service_with_connect_info::<SocketAddr, _>());
            spawn(https_srv)
        };

        tracing::debug!("https layer started");

        tracing::info!("menmosd is up");

        // Now wait until it is time to grab a new certificate.
        let should_quit;
        if let Some(time_to_renew) = time_to_expiration(&pem_name).and_then(|x| x.checked_sub(TMIN))
        {
            should_quit = interruptible_delay(time_to_renew, &mut stop_rx).await;
            tx.send(()).unwrap();
            tx80.send(()).unwrap();
            wait_for_server_stop(http_handle, https_handle).await;
        } else if let Some(time_to_renew) = time_to_expiration(&pem_name) {
            // Presumably we already failed to renew, so let's
            // just keep using our current certificate as long
            // as we can!
            should_quit = interruptible_delay(time_to_renew, &mut stop_rx).await;
            tx.send(()).unwrap();
            tx80.send(()).unwrap();
            wait_for_server_stop(http_handle, https_handle).await;
        } else {
            tracing::warn!("looks like there is an issue with certificate refresh - waiting an hour before retrying...");
            should_quit = interruptible_delay(Duration::from_secs(60 * 60), &mut stop_rx).await;
        }

        if should_quit {
            break Ok(());
        } else {
            tracing::debug!("attempting a certificate renewal");
        }
    }
}

fn time_to_expiration<P: AsRef<StdPath>>(p: P) -> Option<Duration> {
    let file = fs::File::open(p).ok()?;
    Pem::read(BufReader::new(file))
        .ok()?
        .0
        .parse_x509()
        .ok()?
        .tbs_certificate
        .validity
        .time_to_expiration()
}
