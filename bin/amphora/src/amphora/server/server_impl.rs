use std::sync::Arc;

use anyhow::{ensure, Result};

use interface::StorageNode;

use tokio::sync::oneshot;
use tokio::task::{spawn, JoinHandle};

use crate::{CertPath, Config};

use super::filters;

struct ServerProcess {
    pub join_handle: JoinHandle<()>,
    pub tx_stop: oneshot::Sender<()>,
}

pub struct NodeServer<N: StorageNode> {
    _node: Arc<N>,
    handle: ServerProcess,
}

impl<N> NodeServer<N>
where
    N: StorageNode + Send + Sync + 'static,
{
    pub fn new(node: Arc<N>, config: Config, cert_paths: Option<CertPath>) -> Self {
        let (tx_stop, rx) = oneshot::channel();

        let warp_server = warp::serve(filters::all(node.clone(), config.clone()));

        let join_handle = if let Some(certs) = cert_paths {
            tracing::debug!("starting https layer");
            let h = spawn(
                warp_server
                    .tls()
                    .cert_path(&certs.certificate)
                    .key_path(&certs.private_key)
                    .bind_with_graceful_shutdown(([0, 0, 0, 0], config.server.port), async {
                        rx.await.ok();
                    })
                    .1,
            );
            tracing::debug!("https layer started");
            h
        } else {
            tracing::debug!("starting http layer");
            let h = spawn(
                warp_server
                    .bind_with_graceful_shutdown(([0, 0, 0, 0], config.server.port), async {
                        rx.await.ok();
                    })
                    .1,
            );
            tracing::debug!("http layer started");
            h
        };

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
