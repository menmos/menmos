use std::collections::HashSet;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, ensure, Result};
use http::header;
use interface::{BlobInfo, BlobMetaRequest};
use mpart_async::client::MultipartRequest;
use protocol::{directory::storage::MoveRequest, storage::PutResponse};
use tokio::sync::mpsc::{self, error::TrySendError};

use super::concurrent_repository::ConcurrentRepository;

// TODO: Make configurable.
const BUFFER_SIZE: usize = 10;
const RETRY_COUNT: usize = 20;

struct TransferGuard {
    blob_id: String,
    pending_transfers: Arc<Mutex<HashSet<String>>>,
}

impl Drop for TransferGuard {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.pending_transfers.lock() {
            guard.remove(&self.blob_id);
        } else {
            log::error!("error during drop: poisoned mutex");
        }
    }
}

#[derive(Default)]
struct PendingTransfers {
    data: Arc<Mutex<HashSet<String>>>,
}

impl PendingTransfers {
    pub fn start(&self, blob_id: &str) -> Result<Option<TransferGuard>> {
        let mut guard = self.data.lock().map_err(|e| anyhow!("{}", e.to_string()))?;

        if guard.contains(blob_id) {
            return Ok(None);
        }

        (*guard).insert(String::from(blob_id));

        Ok(Some(TransferGuard {
            blob_id: String::from(blob_id),
            pending_transfers: self.data.clone(),
        }))
    }
}

pub struct TransferManager {
    pending_transfers: PendingTransfers,
    handle: tokio::task::JoinHandle<()>,
    tx: mpsc::Sender<(MoveRequest, TransferGuard)>,
}

impl TransferManager {
    pub fn new(repo: Arc<ConcurrentRepository>, index: Arc<sled::Db>, secret_key: String) -> Self {
        let (tx, rx) = mpsc::channel(BUFFER_SIZE);

        let handle = tokio::task::spawn(async move {
            if let Err(e) = TransferManager::consumer_thread(rx, repo, index, secret_key).await {
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

    fn encode_metadata(info: BlobInfo) -> Result<String> {
        let meta_request = BlobMetaRequest::from(info.meta);
        let serialized_meta = serde_json::to_vec(&meta_request)?;
        Ok(base64::encode(&serialized_meta))
    }

    async fn transfer_single(
        repo: Arc<ConcurrentRepository>,
        client: &reqwest::Client,
        request: &MoveRequest,
        index: Arc<sled::Db>,
        secret_key: String,
    ) -> Result<()> {
        // Lock the blob ID manually.
        // This is done this way because we need to keep the blob readlocked while we make a copy of it
        // and transfer it.
        let rwlock = repo.unsafe_lock(&request.blob_id).await;
        let _blob_guard = rwlock.read().await;

        let stream_info = repo
            .unsafe_repository()
            .await
            .get(&request.blob_id, None)
            .await?;

        let info: BlobInfo = if let Some(meta_ivec) = index.get(request.blob_id.as_bytes())? {
            bincode::deserialize(&meta_ivec)?
        } else {
            return Err(anyhow!("failed to update blob size"));
        };

        let encoded_meta = TransferManager::encode_metadata(info)?;

        let mut mpart = MultipartRequest::default();
        let pinned_stream = Pin::from(stream_info.stream);
        mpart.add_stream("src", "upload.bin", "something?", pinned_stream);

        let url = reqwest::Url::parse(&request.destination_url)?;

        let req = client
            .post(url)
            .bearer_auth("TODO: some_token?")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={}", mpart.get_boundary()),
            )
            .header(header::HeaderName::from_static("x-blob-meta"), encoded_meta)
            .body(reqwest::Body::wrap_stream(mpart))
            .build()?;

        let response = client.execute(req).await?;
        let response_bytes = response.bytes().await?;
        let put_response: PutResponse = serde_json::from_slice(&response_bytes)?;

        ensure!(
            put_response.id == request.blob_id,
            "sync returned invalid blob ID while syncing blob '{}'",
            request.blob_id
        );

        Ok(())
    }

    async fn consumer_thread(
        mut rx: mpsc::Receiver<(MoveRequest, TransferGuard)>,
        repo: Arc<ConcurrentRepository>,
        index: Arc<sled::Db>,
        secret_key: String,
    ) -> Result<()> {
        let client = reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(120))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;

        // Note: transfer guard does nothing, but on drop it removes the blob ID from the pending transfers set, preventing duplicates.
        while let Some((request, _transfer_guard)) = rx.recv().await {
            for _ in 0..RETRY_COUNT {
                if let Err(e) = TransferManager::transfer_single(
                    repo.clone(),
                    &client,
                    &request,
                    index.clone(),
                    secret_key.clone(),
                )
                .await
                {
                    log::warn!(
                        "transferring blob '{}' to '{}' failed: {}",
                        &request.blob_id,
                        &request.destination_url,
                        e
                    );
                } else {
                    continue;
                }
            }

            log::error!("exceeded retries while attempting to transfer ")
        }

        Ok(())
    }

    // TODO: Actually call stop from the node.
    pub async fn stop(self) -> Result<()> {
        // Explicitly drop the sender to close the channel and stop the worker.
        {
            let _caught_tx = self.tx;
        }

        // Wait for the thread to stop.
        // TODO: Timeout? Or no? because timeout == potentially dropped syncs
        self.handle.await?;

        Ok(())
    }
}
