use std::io;
use std::ops::Bound;

use anyhow::{bail, Result};

use async_trait::async_trait;

use bytes::Bytes;

use futures::Stream;

use menmos_std::collections::ConcurrentSet;

use repository::{OperationGuard, Repository, StreamInfo};

pub struct ConcurrentRepository {
    repo: Box<dyn Repository + Send + Sync>,
    read_only_blobs: ConcurrentSet<String>, // FIXME(permissions): This is a quick fix before we implement a proper permissions system.
}

impl ConcurrentRepository {
    pub fn new(repo: Box<dyn Repository + Send + Sync>) -> Self {
        Self {
            repo,
            read_only_blobs: ConcurrentSet::new(),
        }
    }

    pub fn set_read_only(&self, blob_id: &str) {
        self.read_only_blobs.insert(String::from(blob_id));
    }

    pub fn remove_read_only(&self, blob_id: &str) {
        self.read_only_blobs.remove(blob_id);
    }
}

#[async_trait]
impl Repository for ConcurrentRepository {
    async fn save(
        &self,
        id: String,
        stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin + 'static>,
        expected_size: u64,
    ) -> Result<Box<dyn OperationGuard>> {
        if self.read_only_blobs.contains(&id) {
            bail!("cannot save blob '{id}': blob is read-only");
        }
        self.repo.save(id, stream, expected_size).await
    }

    async fn write(&self, id: String, range: (Bound<u64>, Bound<u64>), body: Bytes) -> Result<u64> {
        if self.read_only_blobs.contains(&id) {
            bail!("cannot write to blob '{id}': blob is read-only");
        }
        self.repo.write(id, range, body).await
    }

    #[tracing::instrument(skip(self))]
    async fn get(
        &self,
        blob_id: &str,
        range: Option<(Bound<u64>, Bound<u64>)>,
    ) -> Result<StreamInfo> {
        self.repo.get(blob_id, range).await
    }

    async fn delete(&self, blob_id: &str) -> Result<()> {
        if self.read_only_blobs.contains(blob_id) {
            bail!("cannot delete blob '{blob_id}': blob is read-only");
        }
        self.repo.delete(blob_id).await
    }

    async fn fsync(&self, id: String) -> Result<()> {
        if self.read_only_blobs.contains(&id) {
            bail!("cannot fsync blob '{id}': blob is read-only");
        }
        self.repo.fsync(id).await
    }

    async fn available_space(&self) -> Result<Option<u64>> {
        self.repo.available_space().await
    }
}
