use std::sync::Arc;

use anyhow::Result;

use interface::DirectoryNode;

use tokio::sync::mpsc;
use tokio::task::{spawn, JoinHandle};

use crate::config::{Config, ServerSetting};

use super::{context::Context, filters, ssl::use_tls};

pub struct Server {
    node: Arc<Box<dyn DirectoryNode + Send + Sync>>,
    handle: JoinHandle<()>,
    stop_tx: mpsc::Sender<()>,
}

impl Server {
    pub async fn new<N: DirectoryNode + Send + Sync + 'static>(
        cfg: Config,
        node: N,
    ) -> Result<Server> {
        let node: Arc<Box<dyn DirectoryNode + Send + Sync>> = Arc::new(Box::new(node));

        // Create the admin user.
        node.user()
            .register("admin", &cfg.node.admin_password)
            .await?;

        let config = Arc::new(cfg.clone());

        let (stop_tx, mut stop_rx) = mpsc::channel(1);

        let node_cloned = node.clone();
        let join_handle = match cfg.server {
            ServerSetting::Https(https_cfg) => spawn(async move {
                match use_tls(node_cloned, config, https_cfg, stop_rx).await {
                    Ok(_) => {}
                    Err(e) => {
                        log::error!("async error: {}", e)
                    }
                }
            }),
            ServerSetting::Http(http_cfg) => {
                log::info!("starting http layer");
                let server_context = Context {
                    node: node.clone(),
                    config,
                    certificate_info: Arc::new(None),
                };

                let (_addr, srv) = warp::serve(filters::all(server_context))
                    .bind_with_graceful_shutdown(([0, 0, 0, 0], http_cfg.port), async move {
                        stop_rx.recv().await;
                    });

                log::info!("http layer started");
                spawn(srv)
            }
        };

        Ok(Server {
            node,
            handle: join_handle,
            stop_tx,
        })
    }

    pub async fn stop(self) -> Result<()> {
        log::info!("requesting to quit");
        self.stop_tx.send(()).await?;
        self.handle.await?;
        self.node.flush().await?;
        log::info!("exited");

        Ok(())
    }
}
