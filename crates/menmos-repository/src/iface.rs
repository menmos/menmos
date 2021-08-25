use std::{io, ops::Bound};

use anyhow::Result;
use async_trait::async_trait;
use betterstreams::ChunkedStreamInfo;
use bytes::Bytes;
use futures::Stream;

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

    async fn write(&self, id: String, range: (Bound<u64>, Bound<u64>), body: Bytes) -> Result<u64>;

    async fn get(
        &self,
        blob_id: &str,
        range: Option<(Bound<u64>, Bound<u64>)>,
    ) -> Result<ChunkedStreamInfo>;

    async fn delete(&self, blob_id: &str) -> Result<()>;

    async fn fsync(&self, id: String) -> Result<()>;

    async fn available_space(&self) -> Result<Option<u64>>;
}
