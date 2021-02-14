use std::sync::Arc;

use anyhow::Result;

use interface::DirectoryNode;

use tokio::sync::mpsc;
use tokio::task::{spawn, JoinHandle};

use crate::config::{Config, ServerSetting};

use super::{filters, ssl::use_tls};

pub struct Server<N: DirectoryNode> {
    node: Arc<N>,
    handle: JoinHandle<()>,
    stop_tx: mpsc::Sender<()>,
}

impl<N> Server<N>
where
    N: DirectoryNode + Send + Sync + 'static,
{
    pub async fn new(cfg: Config, node: N) -> Result<Server<N>> {
        let n = Arc::from(node);

        let config_cloned = cfg.clone();

        let (stop_tx, mut stop_rx) = mpsc::channel(1);

        let n_cloned = n.clone();
        let join_handle = match cfg.server {
            ServerSetting::HTTPS(https_cfg) => spawn(async move {
                match use_tls(n_cloned, config_cloned.clone(), https_cfg).await {
                    Ok(_) => {}
                    Err(e) => {
                        log::error!("async error: {}", e)
                    }
                }
            }),
            ServerSetting::HTTP(http_cfg) => {
                log::info!("starting http layer");
                let (_addr, srv) = warp::serve(filters::all(n.clone(), config_cloned, None))
                    .bind_with_graceful_shutdown(([0, 0, 0, 0], http_cfg.port), async move {
                        stop_rx.recv().await;
                    });

                log::info!("http layer started");
                spawn(srv)
            }
        };

        Ok(Server {
            node: n,
            handle: join_handle,
            stop_tx,
        })
    }

    pub async fn stop(self) -> Result<()> {
        log::info!("requesting to quit");
        self.stop_tx.send(()).await?;
        self.handle.await?;
        self.node.commit().await?;
        log::info!("exited");

        Ok(())
    }
}
