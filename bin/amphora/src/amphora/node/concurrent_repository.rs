use std::io;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use repository::{Repository, StreamInfo};

use super::stringlock::StringLock;

// TODO: We might want to allow configuring this.
const KEY_LOCKS_MAX_MEMORY: usize = 500 * 1024; // 500kb of memory for the locks, plus whatever for the string IDs themselves.
static KEY_LOCKS_LIFETIME: Duration = Duration::from_secs(60 * 15); // 15 minutes.

pub struct ConcurrentRepository {
    key_locks: StringLock,
    repo: Box<dyn Repository + Send + Sync>,
}

impl ConcurrentRepository {
    pub fn new(repo: Box<dyn Repository + Send + Sync>) -> Self {
        Self {
            key_locks: StringLock::new(KEY_LOCKS_LIFETIME)
                .with_cleanup_trigger(KEY_LOCKS_MAX_MEMORY),
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
}
