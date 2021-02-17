use std::io;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use repository::{Repository, StreamInfo};

use super::stringlock::StringLock;

pub struct ConcurrentRepository {
    key_locks: StringLock,
    repo: Box<dyn Repository + Send + Sync>,
}

impl ConcurrentRepository {
    pub fn new(
        repo: Box<dyn Repository + Send + Sync>,
        lifetime: Duration,
        max_memory: usize,
    ) -> Self {
        Self {
            key_locks: StringLock::new(lifetime).with_cleanup_trigger(max_memory),
            repo,
        }
    }
}

#[async_trait]
impl Repository for ConcurrentRepository {
    async fn save(
        &self,
        id: String,
        size: u64,
        stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin + 'static>,
    ) -> Result<()> {
        let mtx = self.key_locks.get_lock(&id).await;
        let _w_guard = mtx.write().await;
        self.repo.save(id, size, stream).await
    }

    async fn write(&self, id: String, range: interface::Range, body: Bytes) -> Result<u64> {
        let mtx = self.key_locks.get_lock(&id).await;
        let _w_guard = mtx.write().await;
        self.repo.write(id, range, body).await
    }

    async fn get(&self, blob_id: &str, range: Option<interface::Range>) -> Result<StreamInfo> {
        let mtx = self.key_locks.get_lock(&blob_id).await;
        let _r_guard = mtx.read().await;
        self.repo.get(blob_id, range).await
    }

    async fn delete(&self, blob_id: &str) -> Result<()> {
        let mtx = self.key_locks.get_lock(&blob_id).await;
        let _w_guard = mtx.write().await;
        self.repo.delete(blob_id).await
    }

    async fn fsync(&self, id: String) -> Result<()> {
        let mtx = self.key_locks.get_lock(&id).await;
        let _r_guard = mtx.read().await;
        self.repo.fsync(id).await
    }
}
