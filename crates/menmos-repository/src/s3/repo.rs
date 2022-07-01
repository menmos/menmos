use std::io::{self, SeekFrom};
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{ensure, Context, Result};

use async_trait::async_trait;

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{Client, Region};

use betterstreams::DynIoStream;

use bytes::Bytes;

use futures::prelude::*;

use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use super::FileCache;
use crate::iface::OperationGuard;
use crate::{util, Repository, StreamInfo};

/// Get the total length of a blob from a Content-Range header value.
fn get_total_length(range_string: &str) -> Result<u64> {
    let splitted: Vec<_> = range_string.split('/').collect();
    ensure!(splitted.len() == 2, "invalid range response header");

    let total_size = splitted[1].parse::<u64>()?;

    Ok(total_size)
}

struct SaveOperationGuard {
    blob_id: String,
    cache: Arc<FileCache>,
    committed: bool,
}

impl SaveOperationGuard {
    pub fn new(blob_id: String, cache: Arc<FileCache>) -> Self {
        Self {
            blob_id,
            cache,
            committed: false,
        }
    }
}

impl Drop for SaveOperationGuard {
    fn drop(&mut self) {
        if !self.committed {
            if let Err(e) = self.cache.invalidate(&self.blob_id) {
                panic!("failed to rollback save operation: {e}")
            }
        }
    }
}

#[async_trait::async_trait]
impl OperationGuard for SaveOperationGuard {
    async fn commit(&mut self) {
        // FIXME(MEN-164): Related to the comment in FileCache::insert_evict() :
        //                 For now the file cache can evict and destroy a blob
        //                 that is not synced with S3. This is why we consider
        //                 an fsync failure to be an unrecoverable error for now.
        //
        //                 Once this issue is resolved, the file cache will
        //                 periodically retry fsync operations and keep non-synced
        //                 blobs on disk until the sync succeeds, allowing us
        //                 to ignore the error here.
        self.cache
            .fsync(&self.blob_id)
            .await
            .expect("fsync should not fail");

        self.committed = true;
    }
}

struct WriteOperationGuard {
    file_path: PathBuf,
    offset: u64,
    buf: Bytes,
}

impl WriteOperationGuard {
    pub fn new(file_path: PathBuf, offset: u64, buf: Bytes) -> Self {
        Self {
            file_path,
            offset,
            buf,
        }
    }
}

#[async_trait::async_trait]
impl OperationGuard for WriteOperationGuard {
    async fn commit(&mut self) {
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.file_path)
            .await
            .expect("write operation commit should not fail");

        f.seek(SeekFrom::Start(self.offset))
            .await
            .expect("write operation commit should not fail");

        f.write_all(self.buf.as_ref())
            .await
            .expect("write operation commit should not fail");

        f.sync_all()
            .await
            .expect("write operation commit should not fail");
    }
}

struct DeleteOperationGuard {
    blob_id: String,
    bucket: String,
    client: Arc<Client>,
    committed: bool,
}

impl DeleteOperationGuard {
    fn new(blob_id: String, bucket: String, client: Arc<Client>) -> Self {
        Self {
            blob_id,
            bucket,
            client,
            committed: false,
        }
    }
}

impl Drop for DeleteOperationGuard {
    fn drop(&mut self) {
        if !self.committed {
            // FIXME(MEN-165): Remove the blob from the delete precommit structure
            //                 once we have one.
        }
    }
}

#[async_trait::async_trait]
impl OperationGuard for DeleteOperationGuard {
    async fn commit(&mut self) {
        // FIXME(MEN-165): We can't really pre-delete on S3 because moves are slow and expensive.
        //                 What we'll do for now is try the delete in the commit phase and panic
        //                 if it fails. In the future with MEN-165, we want to add a way of persisting
        //                 the S3 operations that failed until they are applied successfully.
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(self.blob_id.to_string())
            .send()
            .await
            .context("failed to delete blob on s3")
            .expect("S3 delete should not fail");

        self.committed = true;
    }
}

pub struct S3Repository {
    bucket: String,

    client: Arc<Client>,
    file_cache: Arc<FileCache>,
}

impl S3Repository {
    pub async fn new(
        bucket: &str,
        region: &str,
        cache_path: &Path,
        max_nb_of_cached_files: usize,
    ) -> Result<Self> {
        let region_provider = RegionProviderChain::first_try(Region::new(String::from(region)))
            .or_default_provider()
            .or_else(Region::new("us-east-1"));

        let shared_config = aws_config::from_env().region(region_provider).load().await;
        let client = Arc::new(Client::new(&shared_config));

        let file_cache = Arc::new(
            FileCache::new(cache_path, max_nb_of_cached_files, bucket, client.clone())
                .context("failed to initialize file cache")?,
        );

        Ok(Self {
            bucket: String::from(bucket),
            client,
            file_cache,
        })
    }
}

#[async_trait]
impl Repository for S3Repository {
    #[tracing::instrument(name = "s3.save", skip(self, stream))]
    async fn save(
        &self,
        id: String,
        stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin + 'static>,
        expected_size: u64,
    ) -> Result<Box<dyn OperationGuard>> {
        tokio::task::block_in_place(|| {
            self.file_cache
                .invalidate(&id)
                .context("failed to invalidate entry from s3 file cache")?;
            Ok::<_, anyhow::Error>(())
        })?;

        self.file_cache.put(&id, stream, expected_size).await?;

        Ok(Box::new(SaveOperationGuard::new(
            id.clone(),
            self.file_cache.clone(),
        )))
    }

    #[tracing::instrument(name = "s3.write", skip(self, body))]
    async fn write(
        &self,
        id: String,
        range: (Bound<u64>, Bound<u64>),
        body: Bytes,
    ) -> Result<(u64, Box<dyn OperationGuard>)> {
        let range = util::bounds_to_range(range, 0, 0);
        let (start, end) = (range.start, range.end);
        ensure!(
            start < end,
            "invalid range, end bound is smaller than start bound"
        );

        let file_path = self.file_cache.get(&id).await?;
        let file_length = file_path.metadata()?.len();
        let new_length = (start + end).max(file_length);

        Ok((
            new_length,
            Box::new(WriteOperationGuard::new(file_path, start, body)),
        ))
    }

    #[tracing::instrument(name = "s3.get", skip(self))]
    async fn get(
        &self,
        blob_id: &str,
        range: Option<(Bound<u64>, Bound<u64>)>,
    ) -> Result<StreamInfo> {
        // First, if the blob is in cache we'll read from there -- much faster.
        if let Some(blob_path) = self.file_cache.contains(blob_id).await {
            tracing::debug!("blob is in cache");
            let file_size = blob_path.metadata()?.len();
            return betterstreams::fs::read_range(
                blob_path,
                range.map(|r| util::bounds_to_range(r, 0, file_size)),
            )
            .await
            .context("failed to read from file cache");
        }

        tracing::debug!("blob is not in cache, falling back on S3");

        // Else we carry on to S3.
        let mut req_builder = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(blob_id.to_string());

        if let Some(r) = range.as_ref() {
            let min_value = match r.start_bound() {
                Bound::Included(i) => *i,
                Bound::Excluded(i) => *i + 1,
                Bound::Unbounded => 0,
            };

            let fmt_max_value = match r.end_bound() {
                Bound::Included(i) => i.to_string(),
                Bound::Excluded(i) => (*i + 1).to_string(),
                Bound::Unbounded => String::default(),
            };

            let range_str = format!("bytes={}-{}", min_value, fmt_max_value);
            req_builder = req_builder.range(range_str);
        }

        let result = req_builder
            .send()
            .await
            .context("s3 GetObject request failed")?;

        let raw_content_length: i64 = result.content_length;
        tracing::trace!(
            content_length = raw_content_length,
            "got GetObject response"
        );

        ensure!(raw_content_length >= 0, "content length cannot be negative");

        let chunk_size = raw_content_length as u64;

        let total_size = if range.is_some() {
            get_total_length(result.content_range.as_ref().unwrap())?
        } else {
            chunk_size
        };

        // S3 returns a custom error type, we use io::error. We need to convert the stream lazily to use our errors, if need be.
        let io_stream: DynIoStream = Box::from(
            result
                .body
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string())),
        );

        Ok(StreamInfo {
            stream: io_stream,
            total_size,
            chunk_size,
        })
    }

    #[tracing::instrument(name = "s3.delete", skip(self))]
    async fn delete(&self, blob_id: &str) -> Result<Box<dyn OperationGuard>> {
        tokio::task::block_in_place(|| {
            self.file_cache
                .invalidate(blob_id)
                .context("failed to invalidate entry from s3 file cache")?;
            Ok::<_, anyhow::Error>(())
        })?;

        Ok(Box::new(DeleteOperationGuard::new(
            String::from(blob_id),
            self.bucket.clone(),
            self.client.clone(),
        )))
    }

    #[tracing::instrument(name = "s3.fsync", skip(self))]
    async fn fsync(&self, id: String) -> Result<()> {
        self.file_cache.fsync(&id).await
    }

    async fn available_space(&self) -> Result<Option<u64>> {
        Ok(None)
    }
}
