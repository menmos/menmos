use std::io;
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, ensure, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::empty, Stream};
use interface::{Blob, BlobInfo, CertificateInfo, StorageNode, Type};
use repository::{Repository, StreamInfo};
use tokio::sync::Mutex;

use super::{
    directory_proxy::DirectoryProxy, node_info::get_info, rebuild, ConcurrentRepository, Config,
};

type RepoBox = Box<dyn Repository + Send + Sync>;

type ConcurrentCertInfo = Arc<Mutex<Option<CertificateInfo>>>;

pub struct Storage {
    config: Config,

    certificates: ConcurrentCertInfo,

    directory: Arc<DirectoryProxy>,

    index: Arc<sled::Db>,
    repo: ConcurrentRepository,
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

        let index = Arc::from(sled::open(&config.node.db_path)?);

        let repo = ConcurrentRepository::new(
            repo,
            Duration::from_secs(config.node.key_locks_lifetime_seconds),
            config.node.key_locks_max_memory,
        );

        let s = Self {
            config,
            directory: proxy,
            index,
            repo,
            certificates,
        };

        Ok(s)
    }

    pub async fn update_registration(&self) -> Result<()> {
        let response = self
            .directory
            .register_storage_node(
                get_info(
                    self.config.server.port,
                    self.config.server.subnet_mask,
                    self.config.node.name.clone(),
                    self.config.node.redirect_ip.clone(),
                )
                .await?,
                &self.config.server.certificate_storage_path,
            )
            .await?;

        // Update the certificate info.
        {
            let mut cert_info_guard = self.certificates.lock().await;
            *cert_info_guard = response.certificate_info;
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
                    log::error!("rebuild failed: {}", e);
                } else {
                    log::info!("rebuild complete");
                }
            });
        }
        Ok(())
    }

    fn is_blob_owned_by(&self, blob_id: &str, username: &str) -> Result<bool> {
        if let Some(blob_info_iv) = self.index.get(blob_id.as_bytes())? {
            let blob_info: BlobInfo = bincode::deserialize(blob_info_iv.as_ref())?;
            Ok(blob_info.owner == username)
        } else {
            Err(anyhow!("not found"))
        }
    }
}

#[async_trait]
impl StorageNode for Storage {
    async fn put(
        &self,
        id: String,
        info: BlobInfo,
        stream: Option<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin>>,
    ) -> Result<()> {
        if let Some(s) = stream {
            self.repo.save(id.clone(), info.meta.size, s).await?;
        }

        self.index
            .insert(id.as_bytes(), bincode::serialize(&info)?)?;

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

        if let Some(meta_ivec) = self.index.get(id.as_bytes())? {
            let mut info: BlobInfo = bincode::deserialize(&meta_ivec)?;
            info.meta.size = new_blob_size;
            self.index
                .insert(id.as_bytes(), bincode::serialize(&info)?)?;

            // Update the config on the directory.
            self.directory
                .index_blob(&id, info, &self.config.node.name)
                .await?;
        } else {
            return Err(anyhow!("failed to update blob size"));
        }

        Ok(())
    }

    async fn get(&self, blob_id: String, range: Option<(Bound<u64>, Bound<u64>)>) -> Result<Blob> {
        let info: BlobInfo = bincode::deserialize(
            self.index
                .get(blob_id.as_bytes())?
                .ok_or_else(|| anyhow!("missing meta"))?
                .as_ref(),
        )?;

        let stream_info = match info.meta.blob_type {
            Type::Directory => StreamInfo {
                stream: Box::from(empty()),
                chunk_size: 0,
                total_size: 0,
            },
            Type::File => self.repo.get(&blob_id, range).await?,
        };

        Ok(Blob {
            stream: stream_info.stream,
            current_chunk_size: stream_info.chunk_size,
            total_blob_size: stream_info.total_size,
            info,
        })
    }

    async fn update_meta(&self, blob_id: String, info: BlobInfo) -> Result<()> {
        // Privilege check.
        ensure!(self.is_blob_owned_by(&blob_id, &info.owner)?, "forbidden");

        self.index
            .insert(blob_id.as_bytes(), bincode::serialize(&info)?)?;
        self.directory
            .index_blob(&blob_id, info, &self.config.node.name)
            .await?;
        Ok(())
    }

    async fn delete(&self, blob_id: String, username: &str) -> Result<()> {
        // Privilege check.
        ensure!(self.is_blob_owned_by(&blob_id, username)?, "forbidden");

        self.index.remove(&blob_id.as_bytes())?;
        self.repo.delete(&blob_id).await?;

        Ok(())
    }

    async fn get_certificates(&self) -> Option<CertificateInfo> {
        let guard = self.certificates.lock().await;
        (*guard).clone()
    }

    async fn fsync(&self, blob_id: String, username: &str) -> Result<()> {
        // Privilege check.
        ensure!(self.is_blob_owned_by(&blob_id, username)?, "forbidden");

        self.repo.fsync(blob_id).await
    }
}
