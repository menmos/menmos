use std::net::SocketAddr;

use anyhow::{ensure, Result};

use axum::Router;
use axum_server::tls_rustls::RustlsConfig;
use axum_server::Handle;

use interface::DynStorageNode;

use tokio::sync::oneshot;
use tokio::task::{spawn, JoinHandle};

use crate::{CertPath, Config};

fn build_router(node: DynStorageNode, config: &Config) -> Router {
    super::layer::wrap(super::router::new(), node, config)
}

struct ServerProcess {
    pub join_handle: JoinHandle<()>,
    pub tx_stop: oneshot::Sender<()>,
}

pub struct NodeServer {
    _node: DynStorageNode,
    handle: ServerProcess,
}

impl NodeServer {
    pub async fn new(node: DynStorageNode, config: Config, cert_paths: Option<CertPath>) -> Self {
        let (tx_stop, rx) = oneshot::channel();

        let router = build_router(node.clone(), &config);

        let join_handle = if let Some(certs) = cert_paths {
            tracing::debug!("starting https layer");
            let rustls_config = RustlsConfig::from_pem_file(certs.certificate, certs.private_key)
                .await
                .unwrap(); // TODO: Fix Unwrap?

            let interrupt_handle = Handle::new();

            {
                let handle = interrupt_handle.clone();
                tokio::spawn(async move {
                    rx.await.ok();
                    tracing::info!("https layer stop signal received");
                    handle.graceful_shutdown(None)
                });
            }

            let https_srv =
                axum_server::bind_rustls(([0, 0, 0, 0], config.server.port).into(), rustls_config)
                    .handle(interrupt_handle)
                    .serve(router.into_make_service_with_connect_info::<SocketAddr>());

            let h = spawn(async move {
                match https_srv.await {
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!("https server error: {e}");
                    }
                }
            });

            tracing::debug!("https layer started");
            h
        } else {
            tracing::debug!("starting http layer");
            let srv = axum::Server::bind(&([0, 0, 0, 0], config.server.port).into())
                .serve(router.into_make_service_with_connect_info::<SocketAddr>())
                .with_graceful_shutdown(async move {
                    rx.await.ok();
                });

            let h = spawn(async move {
                match srv.await {
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!("http server error: {e}")
                    }
                }
            });

            tracing::debug!("http layer started");
            h
        };

        tracing::info!("amphora is up");

        Self {
            _node: node,
            handle: ServerProcess {
                join_handle,
                tx_stop,
            },
        }
    }

    pub async fn stop(self) -> Result<()> {
        ensure!(
            self.handle.tx_stop.send(()).is_ok(),
            "Shutdown request error"
        );
        self.handle.join_handle.await?;
        Ok(())
    }
}
