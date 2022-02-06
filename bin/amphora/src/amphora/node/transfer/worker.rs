use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, ensure, Result};
use http::header;
use interface::BlobMetaRequest;
use mpart_async::client::MultipartRequest;
use protocol::{directory::storage::MoveRequest, storage::PutResponse};
use tokio::sync::mpsc;

use crate::node::{concurrent_repository::ConcurrentRepository, index::Index};

use super::pending::TransferGuard;

const RETRY_COUNT: usize = 10;

pub struct TransferWorker {
    client: reqwest::Client,
    rx: mpsc::Receiver<(MoveRequest, TransferGuard)>,
    repo: Arc<ConcurrentRepository>,
    index: Arc<Index>,
    secret_key: String,
}

impl TransferWorker {
    pub async fn start(
        rx: mpsc::Receiver<(MoveRequest, TransferGuard)>,
        repo: Arc<ConcurrentRepository>,
        index: Arc<Index>,
        secret_key: String,
    ) -> Result<()> {
        let client = reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(120))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;

        let worker = Self {
            client,
            rx,
            repo,
            index,
            secret_key,
        };

        worker.run().await
    }

    fn encode_metadata(&self, blob_id: &str) -> Result<String> {
        let info = self
            .index
            .get(blob_id)?
            .ok_or_else(|| anyhow!("failed to load blob info"))?;

        let meta_request = BlobMetaRequest::from(info.meta);
        let serialized_meta = serde_json::to_vec(&meta_request)?;

        Ok(base64::encode(&serialized_meta))
    }

    fn get_token(&self, username: &str, blob_id: &str) -> Result<String> {
        menmos_auth::make_token(
            &self.secret_key,
            menmos_auth::UserIdentity {
                username: String::from(username),
                admin: false,
                blobs_whitelist: Some(vec![String::from(blob_id)]),
            },
        )
    }

    async fn sync_blob(&self, request: &MoveRequest) -> Result<()> {
        let stream_info = self
            .repo
            .unsafe_repository()
            .await
            .get(&request.blob_id, None)
            .await?;

        let encoded_meta = self.encode_metadata(&request.blob_id)?;

        let mut mpart = MultipartRequest::default();
        let pinned_stream = Pin::from(stream_info.stream);
        mpart.add_stream("src", "upload.bin", "something?", pinned_stream);

        let url = reqwest::Url::parse(&request.destination_url)?;

        // Generate a token on behalf of the blob owner.
        let token = self.get_token(&request.owner_username, &request.blob_id)?;

        let req = self
            .client
            .post(url)
            .bearer_auth(token)
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={}", mpart.get_boundary()),
            )
            .header(header::HeaderName::from_static("x-blob-meta"), encoded_meta)
            .body(reqwest::Body::wrap_stream(mpart))
            .build()?;

        let response = self.client.execute(req).await?;
        let response_bytes = response.bytes().await?;
        let put_response: PutResponse = serde_json::from_slice(&response_bytes)?;

        ensure!(
            put_response.id == request.blob_id,
            "sync returned invalid blob ID while syncing blob '{}'",
            request.blob_id
        );

        Ok(())
    }

    async fn transfer_single(&self, request: &MoveRequest) -> Result<()> {
        // Lock the blob ID manually.
        // This is done this way because we need to keep the blob readlocked while we make a copy of it
        // and transfer it.
        let rwlock = self.repo.unsafe_lock(&request.blob_id).await;
        let _blob_guard = rwlock.read().await;

        self.sync_blob(request).await?;

        // Once our sync is complete, we can delete the blob from our repo safely.
        self.repo
            .unsafe_repository()
            .await
            .delete(&request.blob_id)
            .await?;

        self.index.remove(&request.blob_id)?;

        Ok(())
    }

    async fn run(mut self) -> Result<()> {
        // Note: transfer guard does nothing, but on drop it removes the blob ID from the pending transfers set, preventing duplicates.
        while let Some((request, _transfer_guard)) = self.rx.recv().await {
            let mut try_count = 0;
            loop {
                if let Err(e) = self.transfer_single(&request).await {
                    tracing::warn!(
                        blob_id= ?request.blob_id,
                        destination= ?request.destination_url,
                        "transfer failed: {}",
                        e
                    );
                } else {
                    break;
                }

                try_count += 1;
                if try_count >= RETRY_COUNT {
                    tracing::error!(
                        "exceeded retries while attempting to transfer blob '{}'",
                        request.blob_id
                    );
                }
            }
        }

        Ok(())
    }
}
