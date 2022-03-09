use std::io;
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, ensure, Result};

use async_trait::async_trait;

use bytes::Bytes;

use chrono::Utc;

use futures::{stream::empty, Stream};

use interface::{Blob, BlobInfo, BlobInfoRequest, CertificateInfo, StorageNode, StorageNodeInfo};

use parking_lot::Mutex;

use repository::{Repository, StreamInfo};

use tokio::sync::Mutex as AsyncMutex;

use super::{
    directory_proxy::DirectoryProxy, index::Index, node_info::get_redirect_info, rebuild,
    transfer::TransferManager, ConcurrentRepository, Config,
};

type RepoBox = Box<dyn Repository + Send + Sync>;

type ConcurrentCertInfo = Arc<Mutex<Option<CertificateInfo>>>;

pub struct Storage {
    config: Config,

    certificates: ConcurrentCertInfo,

    directory: Arc<DirectoryProxy>,

    index: Arc<Index>,
    repo: Arc<ConcurrentRepository>,

    transfer_manager: Arc<AsyncMutex<Option<TransferManager>>>,
}

impl Storage {
    pub async fn new(
        config: Config,
        repo: RepoBox,
        certs: Option<CertificateInfo>,
    ) -> Result<Self> {
        let proxy = Arc::from(DirectoryProxy::new(
            &config.directory,
            &config.node.encryption_key,
        )?);

        let certificates = Arc::from(Mutex::from(certs));

        let index = Arc::from(Index::new(&config.node.db_path)?);

        let repo = Arc::from(ConcurrentRepository::new(
            repo,
            Duration::from_secs(config.node.key_locks_lifetime_seconds),
            config.node.key_locks_max_memory,
        ));

        let transfer_manager = TransferManager::new(repo.clone(), index.clone(), config.clone());

        let s = Self {
            config,
            directory: proxy,
            index,
            repo,
            certificates,
            transfer_manager: Arc::new(AsyncMutex::new(Some(transfer_manager))),
        };

        Ok(s)
    }

    pub async fn update_registration(&self) -> Result<()> {
        let redirect_info = get_redirect_info(
            self.config.redirect.subnet_mask,
            self.config.redirect.ip.clone(),
        )
        .await?;

        let current_size = self.index.size();
        let repo_available_space = self.repo.available_space().await?.unwrap_or(u64::MAX);

        let constraint_available_space = self
            .config
            .node
            .maximum_capacity
            .map(|max_cap| {
                if current_size <= max_cap {
                    max_cap - current_size
                } else {
                    0
                }
            })
            .unwrap_or(u64::MAX);

        let real_available_space = constraint_available_space.min(repo_available_space);

        let node_info = StorageNodeInfo {
            id: self.config.node.name.clone(),
            redirect_info,
            port: self.config.server.port,
            size: self.index.size(),
            available_space: real_available_space,
        };

        let response = self
            .directory
            .register_storage_node(node_info, &self.config.server.certificate_storage_path)
            .await?;

        // Update the certificate info.
        {
            let mut cert_info_guard = self.certificates.lock();
            *cert_info_guard = response.certificate_info;
        }

        // Enqueue the requested transfers.
        if let Some(transfer_manager) = &(*self.transfer_manager.lock().await) {
            for move_request in response.move_requests {
                transfer_manager.move_blob(move_request).await?;
            }
        } else {
            panic!("invalid state: received a request but transfer manager is not running");
        }

        // Trigger the rebuild task.
        if response.rebuild_requested {
            let params = rebuild::Params {
                storage_node_name: self.config.node.name.clone(),
                directory_host_url: self.config.directory.url.clone(),
                directory_host_port: self.config.directory.port,
            };

            let proxy_cloned = self.directory.clone();
            let db_cloned = self.index.clone();

            // TODO: Hold a join handle to this and refuse stopping the storage node until it completes.
            tokio::task::spawn(async move {
                if let Err(e) = rebuild::execute(params, proxy_cloned, db_cloned).await {
                    tracing::error!("rebuild failed: {}", e);
                } else {
                    tracing::info!("rebuild complete");
                }
            });
        }
        Ok(())
    }

    pub async fn stop_transfers(&self) -> Result<()> {
        let manager = {
            let mut manager_guard = self.transfer_manager.lock().await;
            (*manager_guard)
                .take()
                .ok_or_else(|| anyhow!("cannot stop transfers: transfers are already stopped"))?
        };

        manager.stop().await
    }

    fn is_blob_owned_by(&self, blob_id: &str, username: &str) -> Result<bool> {
        let blob_info = self
            .index
            .get(blob_id)?
            .ok_or_else(|| anyhow!("not found"))?;

        Ok(blob_info.owner == username)
    }
}

#[async_trait]
impl StorageNode for Storage {
    async fn put(
        &self,
        id: String,
        info_request: BlobInfoRequest,
        stream: Option<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin>>,
    ) -> Result<()> {
        let actual_blob_size = if let Some(s) = stream {
            self.repo.save(id.clone(), s).await?
        } else {
            0
        };

        // We have a mismatch between the actual size of the stream and the size declared by the user.
        // Clean up & stop everything.
        if actual_blob_size != info_request.size {
            self.repo.delete(&id).await?;
            bail!(
                "size mismatch: broadcasted={} actual={}",
                info_request.size,
                actual_blob_size
            );
        }

        let (created_at, modified_at) = if let Some(old_info) = self.index.get(&id)? {
            (old_info.meta.created_at, old_info.meta.modified_at)
        } else {
            let date = Utc::now();
            (date, date)
        };

        let info = info_request.into_blob_info(created_at, modified_at);
        self.index.insert(&id, &info)?;

        self.directory
            .index_blob(&id, info, &self.config.node.name)
            .await?;

        Ok(())
    }

    async fn write(
        &self,
        id: String,
        range: (Bound<u64>, Bound<u64>),
        body: Bytes,
        username: &str,
    ) -> Result<()> {
        // Privilege check.
        ensure!(self.is_blob_owned_by(&id, username)?, "forbidden");

        // Write the diff
        let new_blob_size = self.repo.write(id.clone(), range, body).await?;

        // Update the index.
        if let Some(mut info) = self.index.get(&id)? {
            info.meta.modified_at = Utc::now();
            info.meta.size = new_blob_size;
            self.index.insert(&id, &info)?;

            // Update the meta on the directory.
            self.directory
                .index_blob(&id, info, &self.config.node.name)
                .await?;
        } else {
            return Err(anyhow!("failed to update blob size"));
        }

        Ok(())
    }

    async fn get(&self, blob_id: String, range: Option<(Bound<u64>, Bound<u64>)>) -> Result<Blob> {
        // TODO: Clip the bounds to the real blob?
        let info: BlobInfo = self
            .index
            .get(&blob_id)?
            .ok_or_else(|| anyhow!("missing meta"))?;

        let stream_info = if info.meta.size == 0 {
            StreamInfo {
                stream: Box::new(empty()),
                chunk_size: 0,
                total_size: 0,
            }
        } else {
            self.repo.get(&blob_id, range).await?
        };

        Ok(Blob {
            stream: stream_info.stream,
            current_chunk_size: stream_info.chunk_size,
            total_blob_size: stream_info.total_size,
            info,
        })
    }

    async fn update_meta(&self, blob_id: String, info_request: BlobInfoRequest) -> Result<()> {
        // Privilege check.
        ensure!(
            self.is_blob_owned_by(&blob_id, &info_request.owner)?,
            "forbidden"
        );

        if let Some(old_info) = self.index.get(&blob_id)? {
            let mut info = info_request.into_blob_info(old_info.meta.created_at, Utc::now());
            info.meta.size = old_info.meta.size; // carry-over the old size because changing the metadata doesn't change the size
            self.index.insert(&blob_id, &info)?;

            self.directory
                .index_blob(&blob_id, info, &self.config.node.name)
                .await?;
        } else {
            return Err(anyhow!("cannot update metadata for non-existent blob"));
        }

        Ok(())
    }

    async fn delete(&self, blob_id: String, username: &str) -> Result<()> {
        // Privilege check.
        ensure!(self.is_blob_owned_by(&blob_id, username)?, "forbidden");

        self.index.remove(&blob_id)?;
        self.repo.delete(&blob_id).await?;

        // Delete the blob on the directory.
        self.directory
            .delete_blob(&blob_id, &self.config.node.name)
            .await?;

        Ok(())
    }

    async fn get_certificates(&self) -> Option<CertificateInfo> {
        let guard = self.certificates.lock();
        (*guard).clone()
    }

    async fn fsync(&self, blob_id: String, username: &str) -> Result<()> {
        // Privilege check.
        ensure!(self.is_blob_owned_by(&blob_id, username)?, "forbidden");

        self.repo.fsync(blob_id).await
    }

    async fn flush(&self) -> Result<()> {
        self.index.flush().await
    }
}
