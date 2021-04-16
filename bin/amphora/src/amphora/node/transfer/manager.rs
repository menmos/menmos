use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};

use protocol::directory::storage::MoveRequest;
use tokio::sync::mpsc::{self, error::TrySendError};

use super::{PendingTransfers, TransferGuard, TransferWorker};
use crate::{node::ConcurrentRepository, Config};

pub struct TransferManager {
    pending_transfers: PendingTransfers,
    handle: tokio::task::JoinHandle<()>,
    tx: mpsc::Sender<(MoveRequest, TransferGuard)>,
}

impl TransferManager {
    pub fn new(repo: Arc<ConcurrentRepository>, index: Arc<sled::Db>, config: Config) -> Self {
        let (tx, rx) = mpsc::channel(config.node.move_request_buffer_size);

        let handle = tokio::task::spawn(async move {
            if let Err(e) = TransferWorker::start(rx, repo, index, config.node.encryption_key).await
            {
                log::error!("transfer manager exited unexpectedly: {}", e);
            }
        });

        Self {
            pending_transfers: Default::default(),
            handle,
            tx,
        }
    }

    pub async fn move_blob(&self, move_request: MoveRequest) -> Result<()> {
        if let Some(transfer_guard) = self.pending_transfers.start(&move_request.blob_id)? {
            if let Err(TrySendError::Closed(_)) = self.tx.try_send((move_request, transfer_guard)) {
                Err(anyhow!("sent sync but sync channel is closed"))
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }

    pub async fn stop(self) -> Result<()> {
        // Explicitly drop the sender to close the channel and stop the worker.
        {
            let _caught_tx = self.tx;
        }

        // Wait for the thread to stop.
        tokio::time::timeout(Duration::from_secs(60 * 5), self.handle).await??;

        Ok(())
    }
}
