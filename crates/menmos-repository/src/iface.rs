use std::{io, ops::Bound};

use anyhow::Result;
use async_trait::async_trait;
use betterstreams::ChunkedStreamInfo;
use bytes::Bytes;
use futures::Stream;

#[async_trait]
pub trait Repository {
    /// Writes a whole blob from a stream, overwriting if it already exists.
    ///
    /// A repository implementing save should:
    ///     - Consume the stream without overwriting the old blob (if it exists).
    ///     - Validate that the amount of bytes consumed is equal to the expected size (throwing if not).
    ///     - Write the contents of the stream to its final destination, overwriting if necessary.
    ///
    /// If an error is returned, the blob must not have been modified.
    async fn save(
        &self,
        id: String,
        mut stream: Box<
            dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin + 'static,
        >,
        expected_size: u64,
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
