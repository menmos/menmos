mod handlers;
mod layer;
mod router;
mod ssl;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;

use axum::Router;

use interface::{CertificateInfo, DirectoryNode, DynDirectoryNode};

use tokio::sync::mpsc;
use tokio::task::{spawn, JoinHandle};

use crate::config::{Config, ServerSetting};

pub(crate) fn build_router(
    config: Arc<Config>,
    node: DynDirectoryNode,
    certificate_info: Arc<Option<CertificateInfo>>,
) -> Router {
    layer::wrap(router::new(), config, node, certificate_info)
}

pub struct Server {
    node: Arc<dyn DirectoryNode + Send + Sync>,
    handle: JoinHandle<()>,
    stop_tx: mpsc::Sender<()>,
}

impl Server {
    pub async fn new<N: DirectoryNode + Send + Sync + 'static>(
        cfg: Config,
        node: N,
    ) -> Result<Server> {
        let node: Arc<dyn DirectoryNode + Send + Sync> = Arc::new(node);

        // Create the admin user.
        node.user()
            .register("admin", &cfg.node.admin_password)
            .await?;

        let config = Arc::new(cfg.clone());

        let (stop_tx, mut stop_rx) = mpsc::channel(1);

        let join_handle = match cfg.server {
            ServerSetting::Http(http_cfg) => {
                tracing::debug!("starting http layer");

                let srv = axum::Server::bind(&([0, 0, 0, 0], http_cfg.port).into())
                    .serve(
                        build_router(config, node.clone(), Arc::new(None))
                            .into_make_service_with_connect_info::<SocketAddr>(),
                    )
                    .with_graceful_shutdown(async move {
                        stop_rx.recv().await;
                    });

                tracing::debug!("http layer started");
                tracing::info!("menmosd is up");
                spawn(async move {
                    match srv.await {
                        Ok(_) => {}
                        Err(e) => {
                            tracing::error!("http server error: {e}");
                        }
                    }
                })
            }
            ServerSetting::Https(https_cfg) => {
                let node = node.clone();
                spawn(async move {
                    match ssl::use_tls(node, config, https_cfg, stop_rx).await {
                        Ok(_) => {}
                        Err(e) => {
                            tracing::error!("https server error: {e}")
                        }
                    }
                })
            }
        };

        Ok(Server {
            node,
            handle: join_handle,
            stop_tx,
        })
    }

    pub async fn stop(self) -> Result<()> {
        tracing::info!("requesting to quit");
        self.stop_tx.send(()).await?;
        self.handle.await?;
        self.node.flush().await?;
        tracing::info!("exited");

        Ok(())
    }
}
