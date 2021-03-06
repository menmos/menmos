use std::io::{self, SeekFrom};
use std::ops::{Bound, RangeBounds};
use std::path::PathBuf;

use anyhow::{anyhow, ensure, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::prelude::*;
use rusoto_core::Region;
use rusoto_s3::{
    DeleteObjectRequest, GetObjectRequest, PutObjectRequest, S3Client, StreamingBody, S3,
};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use super::FileCache;
use crate::{util, Repository, StreamInfo};

/// Get the total length of a blob from a Content-Range header value.
fn get_total_length(range_string: &str) -> Result<u64> {
    let splitted: Vec<_> = range_string.split('/').collect();
    ensure!(splitted.len() == 2, "invalid range response header");

    let total_size = splitted[1].parse::<u64>()?;

    Ok(total_size)
}

pub struct S3Repository {
    bucket: String,
    client: S3Client,

    file_cache: FileCache,
}

impl S3Repository {
    pub fn new<S: Into<String>, P: Into<PathBuf>>(
        bucket: S,
        cache_path: P,
        max_nb_of_cached_files: usize,
    ) -> Result<Self> {
        let client = S3Client::new(Region::UsEast1); // TODO: Make configurable.
        let bucket_str: String = bucket.into();

        let file_cache = FileCache::new(
            cache_path,
            max_nb_of_cached_files,
            &bucket_str,
            client.clone(),
        )?;

        Ok(Self {
            bucket: bucket_str,
            client,
            file_cache,
        })
    }
}

#[async_trait]
impl Repository for S3Repository {
    async fn save(
        &self,
        id: String,
        size: u64,
        stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin + 'static>,
    ) -> Result<()> {
        self.file_cache.invalidate(&id).await?;

        let _result = self
            .client
            .put_object(PutObjectRequest {
                bucket: self.bucket.clone(),
                key: id,
                body: Some(StreamingBody::new(stream)),
                content_length: Some(size as i64),
                ..Default::default()
            })
            .await?;
        Ok(())
    }

    async fn write(&self, id: String, range: (Bound<u64>, Bound<u64>), body: Bytes) -> Result<u64> {
        let file_path = self.file_cache.get(&id).await?;
        let range = util::bounds_to_range(range, 0, 0);
        let (start, end) = (range.start, range.end);
        ensure!(start < end, "invalid range");

        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&file_path)
                .await?;
            f.seek(SeekFrom::Start(start)).await?;
            f.write_all(body.as_ref()).await?;
            f.sync_all().await?;
        }

        let file_length = file_path.metadata()?.len();

        Ok(file_length)
    }

    async fn get(
        &self,
        blob_id: &str,
        range: Option<(Bound<u64>, Bound<u64>)>,
    ) -> Result<StreamInfo> {
        // First, if the blob is in cache we'll read from there -- much faster.
        if let Some(blob_path) = self.file_cache.contains(blob_id).await {
            let file_size = blob_path.metadata()?.len();
            return betterstreams::fs::read_range(
                blob_path,
                range.map(|r| util::bounds_to_range(r, 0, file_size)),
            )
            .await;
        }

        // Else we carry on to S3.
        let mut get_request = GetObjectRequest {
            bucket: self.bucket.clone(),
            key: blob_id.to_string(),
            ..Default::default()
        };

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
            get_request.range = Some(range_str)
        }

        let result = self.client.get_object(get_request).await?;

        let (chunk_size, total_size) = if range.is_some() {
            let chunk_size = result.content_length.unwrap() as u64; // TODO: No unwraps + handle numeric cast.
            let total_size = get_total_length(&result.content_range.as_ref().unwrap())?;
            (chunk_size, total_size)
        } else {
            // If the user didnt request a range, the chunk size and the file size are equal.
            let content_length = result.content_length.unwrap() as u64; // TODO: No unwraps please and numeric cast.
            (content_length, content_length)
        };

        if let Some(bytestream) = result.body {
            Ok(StreamInfo {
                stream: Box::from(bytestream),
                total_size,
                chunk_size,
            })
        } else {
            Err(anyhow!("missing stream"))
        }
    }

    async fn delete(&self, blob_id: &str) -> Result<()> {
        self.file_cache.invalidate(&blob_id).await?;

        let delete_request = DeleteObjectRequest {
            bucket: self.bucket.clone(),
            key: blob_id.to_string(),
            ..Default::default()
        };

        self.client.delete_object(delete_request).await?;

        Ok(())
    }

    async fn fsync(&self, id: String) -> Result<()> {
        // TODO: Trigger fsync asynchronously so it doesn't block the call.
        // TODO: Trigger fsync periodically for cache keys, and every time on cache eviction.
        if let Some(path) = self.file_cache.contains(&id).await {
            log::info!("begin fsync on {}", &id);
            let f = fs::File::open(&path).await?;
            let file_length = path.metadata()?.len();
            let _result = self
                .client
                .put_object(PutObjectRequest {
                    bucket: self.bucket.clone(),
                    key: id.clone(),
                    body: Some(StreamingBody::new_with_size(
                        betterstreams::util::reader_to_iostream(f),
                        file_length as usize,
                    )),
                    ..Default::default()
                })
                .await?;
            log::info!("fsync on {} complete", id);
        }

        Ok(())
    }
}
