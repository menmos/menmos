use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;

use interface::{CertificateInfo, StorageNode};

use tokio::{sync::mpsc, task::JoinHandle};

use crate::{constants, make_node, CertPath, Config};

use super::server_impl::NodeServer;

fn load_certificates<P: AsRef<Path>>(cert_directory: P) -> Option<CertificateInfo> {
    CertificateInfo::from_path(
        cert_directory
            .as_ref()
            .join(constants::CERTIFICATE_FILE_NAME),
        cert_directory
            .as_ref()
            .join(constants::PRIVATE_KEY_FILE_NAME),
    )
    .ok()
}

async fn block_until_cert_change<N: StorageNode>(
    n: Arc<N>,
    initial_certificates: Option<CertificateInfo>,
) {
    loop {
        let new_certs_maybe = n.get_certificates().await;

        if initial_certificates != new_certs_maybe {
            break;
        }

        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

pub struct RebootableServer {
    stop_tx: mpsc::Sender<()>,
    handle: JoinHandle<()>,
}

impl RebootableServer {
    pub fn new(config: Config) -> Self {
        let (stop_tx, stop_rx) = mpsc::channel(1);
        let handle = tokio::task::spawn(async move {
            RebootableServer::server_loop_task(config, stop_rx).await;
        });
        Self { stop_tx, handle }
    }

    async fn run_until_refresh_or_stop(
        cfg: Config,
        stop_rx: &mut mpsc::Receiver<()>,
    ) -> Result<bool> {
        let certs = load_certificates(&cfg.server.certificate_storage_path);

        let storage_node = Arc::from(make_node(cfg.clone(), certs.clone()).await?);

        // Start the periodic registration task.
        let (registration_handle, registration_stop) = {
            let node_cloned = storage_node.clone();
            let checkin_frequency = Duration::from_secs(cfg.node.checkin_frequency_seconds);

            let (stop_tx, mut stop_rx) = tokio::sync::mpsc::channel(1);

            let job_handle = tokio::task::spawn(async move {
                loop {
                    match node_cloned.update_registration().await {
                        Ok(_) => {
                            tracing::debug!("directory registration complete")
                        }
                        Err(e) => {
                            tracing::error!("failed to update registration: {}", e)
                        }
                    }

                    let stop_future = stop_rx.recv();
                    let delay = tokio::time::sleep(checkin_frequency);
                    tokio::pin!(delay);

                    let should_stop = tokio::select! {
                        _ = delay => {
                            false
                        }
                        _ = stop_future => {
                            true
                        }
                    };

                    if should_stop {
                        break;
                    }
                }
            });
            (job_handle, stop_tx)
        };

        // Start the server.
        let cert_paths = if certs.is_some() {
            Some(CertPath {
                certificate: cfg
                    .server
                    .certificate_storage_path
                    .join(constants::CERTIFICATE_FILE_NAME),
                private_key: cfg
                    .server
                    .certificate_storage_path
                    .join(constants::PRIVATE_KEY_FILE_NAME),
            })
        } else {
            None
        };

        let s = NodeServer::new(storage_node.clone(), cfg.clone(), cert_paths);

        let cert_change_validator = block_until_cert_change(storage_node.clone(), certs);
        let stop_signal = stop_rx.recv();

        let should_terminate = tokio::select! {
            _ = cert_change_validator => {
                false
            }
            _ = stop_signal => {
                tracing::info!("received stop signal");
                true
            }
        };

        tracing::debug!("waiting for storage server to stop");
        s.stop().await?;
        tracing::debug!("storage server stopped");

        tracing::debug!("waiting for transfers to stop");
        storage_node.stop_transfers().await?;
        tracing::debug!("transfers stopped");

        tracing::debug!("waiting for registration thread to stop");
        registration_stop.send(()).await?;
        registration_handle.await?;
        tracing::debug!("registration thread stopped");

        Ok(should_terminate)
    }

    async fn server_loop_task(cfg: Config, mut stop_rx: mpsc::Receiver<()>) {
        loop {
            match RebootableServer::run_until_refresh_or_stop(cfg.clone(), &mut stop_rx).await {
                Ok(should_terminate) => {
                    if should_terminate {
                        return;
                    }
                }
                Err(e) => {
                    tracing::error!("error with the main server loop: {}", e);
                    return;
                }
            }
        }
    }

    pub async fn stop(self) -> Result<()> {
        self.stop_tx.send(()).await?;
        self.handle.await?;
        Ok(())
    }
}
