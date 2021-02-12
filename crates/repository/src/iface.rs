use std::io;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;

pub struct StreamInfo {
    pub stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + 'static>,
    pub current_chunk_size: u64,
    pub total_blob_size: u64,
}

#[async_trait]
pub trait Repository {
    async fn save(
        &self,
        id: String,
        size: u64,
        mut stream: Box<
            dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin + 'static,
        >,
    ) -> Result<()>;

    async fn write(&self, id: String, range: interface::Range, body: Bytes) -> Result<u64>;

    async fn get(&self, blob_id: &str, range: Option<interface::Range>) -> Result<StreamInfo>;

    async fn delete(&self, blob_id: &str) -> Result<()>;
}
