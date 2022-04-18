use std::io;
use std::ops::Bound;
use std::sync::Arc;

use anyhow::{anyhow, ensure, Result};

use async_trait::async_trait;

use bytes::Bytes;

use futures::{stream::empty, Stream};

use interface::{Blob, BlobInfo, BlobInfoRequest, CertificateInfo, StorageNode, StorageNodeInfo};

use menmos_std::sync::ShardedMutex;
use menmos_std::tx;

use parking_lot::Mutex;

use repository::{Repository, StreamInfo};

use time::OffsetDateTime;
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

    shard_lock: ShardedMutex,
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

        let repo = Arc::from(ConcurrentRepository::new(repo));

        let transfer_manager = TransferManager::new(repo.clone(), index.clone(), config.clone());

        // TODO: Tune this (and allow runtime tuning).
        let shard_lock = ShardedMutex::new(10, 0.01);

        let s = Self {
            config,
            directory: proxy,
            index,
            repo,
            certificates,
            transfer_manager: Arc::new(AsyncMutex::new(Some(transfer_manager))),
            shard_lock,
        };

        Ok(s)
    }

    #[tracing::instrument(name = "node.update_registration", skip(self), fields(otel.kind = "Client"))]
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
    /// Creates / overwrites a blob in this storage node.
    ///
    /// # Insertion process
    /// 0. Writelock the mutex shard corresponding to the blob.
    /// 1. Start a transaction with `try_rollback`
    /// 2. Write the blob metadata in the local sled store.
    /// 3. Enqueue an operation in the transaction to revert the metadata mutation in case of failure.
    /// 4. Consume and pre-commit the stream to the repository (which gives us back a [`repository::OperationGuard`])
    /// 5. Send the new blob metadata to the directory node
    /// 6. Finally, if everything went OK, commit the blob to the repository.
    /// 7. Release the shard lock and return.
    #[tracing::instrument(name = "node.put", skip(self, info_request, stream))]
    async fn put(
        &self,
        id: String,
        info_request: BlobInfoRequest,
        stream: Option<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin>>,
    ) -> Result<()> {
        let _guard = self.shard_lock.write(&id).await;

        tx::try_rollback(move |tx_state| async move {
            let (created_at, modified_at, old_info) = if let Some(old_info) = self.index.get(&id)? {
                (
                    old_info.meta.created_at,
                    old_info.meta.modified_at,
                    Some(old_info),
                )
            } else {
                let date = OffsetDateTime::now_utc();
                (date, date, None)
            };

            let size = info_request.size;
            let info = info_request.into_blob_info(created_at, modified_at);
            self.index.insert(&id, &info)?;

            // Add the meta revert step.
            tx_state
                .complete({
                    let id = id.clone();
                    let old_info = old_info.clone();
                    let index = self.index.clone();
                    Box::pin(async move {
                        if let Some(info) = old_info {
                            // We did an update so we'll revert to the old one
                            index.insert(&id, &info)?;
                        } else {
                            // We did an insert so we'll delete
                            index.remove(&id)?;
                        }

                        Ok(())
                    })
                })
                .await;

            let save_operation = if let Some(s) = stream {
                Some(self.repo.save(id.clone(), s, size).await?)
            } else {
                None
            };

            self.directory
                .index_blob(&id, info, &self.config.node.name)
                .await?;

            if let Some(mut op) = save_operation {
                op.commit().await;
            }

            Ok(())
        })
        .await
    }

    #[tracing::instrument(name = "node.write", skip(self, body), fields(buf_size=?body.len()))]
    async fn write(
        &self,
        id: String,
        range: (Bound<u64>, Bound<u64>),
        body: Bytes,
        username: &str,
    ) -> Result<()> {
        let _guard = self.shard_lock.write(&id).await;

        // Privilege check.
        ensure!(self.is_blob_owned_by(&id, username)?, "forbidden");

        // Update the index.
        if let Some(info) = self.index.get(&id)? {
            tx::try_rollback(move |tx_state| async move {
                let (new_blob_size, mut op_guard) =
                    self.repo.write(id.clone(), range, body).await?;

                let mut new_info = info.clone();
                new_info.meta.modified_at = OffsetDateTime::now_utc();
                new_info.meta.size = new_blob_size;

                self.index.insert(&id, &new_info)?;
                tx_state
                    .complete({
                        let index = self.index.clone();
                        let id = id.clone();
                        Box::pin(async move {
                            index.insert(&id, &info)?;
                            Ok(())
                        })
                    })
                    .await;

                // Update the meta on the directory.
                self.directory
                    .index_blob(&id, new_info, &self.config.node.name)
                    .await?;

                op_guard.commit().await;
                Ok(())
            })
            .await
        } else {
            Err(anyhow!("blob meta did not exist"))
        }
    }

    #[tracing::instrument(name = "node.get", skip(self))]
    async fn get(&self, blob_id: String, range: Option<(Bound<u64>, Bound<u64>)>) -> Result<Blob> {
        let _guard = self.shard_lock.read(&blob_id).await;

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

    #[tracing::instrument(name = "node.update_meta", skip(self, info_request))]
    async fn update_meta(&self, blob_id: String, info_request: BlobInfoRequest) -> Result<()> {
        let _guard = self.shard_lock.write(&blob_id).await;

        // Privilege check.
        ensure!(
            self.is_blob_owned_by(&blob_id, &info_request.owner)?,
            "forbidden"
        );

        tx::try_rollback(move |tx_state| async move {
            if let Some(old_info) = self.index.get(&blob_id)? {
                let mut info = info_request
                    .into_blob_info(old_info.meta.created_at, OffsetDateTime::now_utc());

                info.meta.size = old_info.meta.size; // carry-over the old size because changing the metadata doesn't change the size

                self.index.insert(&blob_id, &info)?;
                tx_state
                    .complete({
                        let blob_id = blob_id.clone();
                        let index = self.index.clone();
                        Box::pin(async move {
                            index.insert(&blob_id, &old_info)?;
                            Ok(())
                        })
                    })
                    .await;

                self.directory
                    .index_blob(&blob_id, info, &self.config.node.name)
                    .await?;
            } else {
                return Err(anyhow!("cannot update metadata for non-existent blob"));
            }

            Ok(())
        })
        .await
    }

    /// Deletes a blob from this storage node.
    ///
    /// # Deletion Process
    /// 0. Writelock the mutex shard corresponding to the blob.
    /// 1. Check that the blob exists and that it is owned by the user making the request.
    /// 2. Remove the blob metadata from the local sled store.
    /// 3. Enqueue an operation in the transaction to revert the metadata mutation in case of failure.
    /// 4. Pre-delete the blob in the repository (which gives us back a [`repository::OperationGuard`])
    /// 5. Delete the metadata on the directory node.
    /// 6. If everything went OK, commit the deletion.
    /// 7. Release the shard lock and return.
    #[tracing::instrument(name = "node.delete", skip(self))]
    async fn delete(&self, blob_id: String, username: &str) -> Result<()> {
        let _guard = self.shard_lock.write(&blob_id).await;

        // Privilege check.
        ensure!(self.is_blob_owned_by(&blob_id, username)?, "forbidden");

        tx::try_rollback(move |tx_state| async move {
            let blob_info = self
                .index
                .remove(&blob_id)?
                .ok_or_else(|| anyhow!("missing blob meta for {blob_id}"))?;

            // Add step to rollback the index deletion
            tx_state
                .complete({
                    let blob_id = blob_id.clone();
                    let blob_info = blob_info.clone();
                    let index = self.index.clone();
                    Box::pin(async move {
                        index.insert(&blob_id, &blob_info)?;
                        Ok(())
                    })
                })
                .await;

            let mut delete_op = self.repo.delete(&blob_id).await?;

            // Delete the blob on the directory.
            self.directory
                .delete_blob(&blob_id, &self.config.node.name)
                .await?;

            delete_op.commit().await;

            Ok(())
        })
        .await
    }

    async fn get_certificates(&self) -> Option<CertificateInfo> {
        let guard = self.certificates.lock();
        (*guard).clone()
    }

    #[tracing::instrument(name = "node.fsync", skip(self))]
    async fn fsync(&self, blob_id: String, username: &str) -> Result<()> {
        let _guard = self.shard_lock.write(&blob_id).await;

        // Privilege check.
        ensure!(self.is_blob_owned_by(&blob_id, username)?, "forbidden");

        self.repo.fsync(blob_id).await
    }

    #[tracing::instrument(name = "node.flush", skip(self))]
    async fn flush(&self) -> Result<()> {
        self.index.flush().await
    }
}
