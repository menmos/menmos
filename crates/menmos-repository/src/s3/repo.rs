use std::io::{self, SeekFrom, Write};
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::pin::Pin;

use anyhow::{anyhow, bail, ensure, Context, Result};
use async_trait::async_trait;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::model::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::{Client, Region};
use betterstreams::DynIoStream;
use bytes::buf::Writer;
use bytes::{BufMut, Bytes, BytesMut};
use futures::prelude::*;
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

    client: Client,
    file_cache: FileCache,
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
        let client = Client::new(&shared_config);

        let file_cache = FileCache::new(cache_path, max_nb_of_cached_files, bucket, client.clone())
            .context("failed to initialize file cache")?;

        Ok(Self {
            bucket: String::from(bucket),
            client,
            file_cache,
        })
    }

    #[tracing::instrument(name = "s3.flush_part", level = "trace", skip(self, buf_writer))]
    async fn flush_part(
        &self,
        running_size: usize,
        buf_writer: Writer<BytesMut>,
        part_id: i32,
        upload_id: &str,
        id: &str,
    ) -> Result<CompletedPart> {
        let result = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .content_length(running_size as i64)
            .key(id)
            .body(ByteStream::from(buf_writer.into_inner().freeze()))
            .part_number(part_id)
            .upload_id(upload_id)
            .send()
            .await
            .context("s3 repository flush error")?;

        tracing::trace!("part flushed");

        Ok(CompletedPart::builder()
            .set_e_tag(result.e_tag)
            .part_number(part_id)
            .build())
    }

    #[tracing::instrument(name = "s3.do_multipart", level = "trace", skip(self, stream))]
    async fn do_multipart(
        &self,
        id: String,
        upload_id: String,
        stream: betterstreams::DynIoStream,
    ) -> Result<(CompletedMultipartUpload, u64)> {
        let mut part_id = 1;

        let mut parts_builder = CompletedMultipartUpload::builder();

        let mut buf_writer = BytesMut::new().writer();
        let mut running_size = 0;
        let mut total_size = 0_u64;

        let mut stream = Pin::from(stream);
        while let Some(part) = stream.try_next().await? {
            tracing::trace!(size = part.len(), "got stream part");

            buf_writer.write_all(&part)?;
            running_size += part.len();
            total_size += part.len() as u64;

            if running_size <= 5 * 1024 * 1024 {
                tracing::trace!(
                    "running size of {} is smaller than {}, continuing",
                    running_size,
                    5 * 1024 * 1024
                );
                continue;
            }

            tracing::trace!("flushing current part");
            let completed_part = self
                .flush_part(running_size, buf_writer, part_id, &upload_id, &id)
                .await?;

            parts_builder = parts_builder.parts(completed_part);

            part_id += 1;
            running_size = 0;
            buf_writer = BytesMut::new().writer();

            tracing::trace!("next part id={part_id}");
        }

        // Flush the last part if required
        if running_size > 0 {
            tracing::trace!(
                "finished consuming stream but some data is left over, sending one last part"
            );
            let completed_part = self
                .flush_part(running_size, buf_writer, part_id, &upload_id, &id)
                .await?;

            parts_builder = parts_builder.parts(completed_part);
        }

        Ok((parts_builder.build(), total_size))
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
    ) -> Result<()> {
        self.file_cache
            .invalidate(&id)
            .await
            .context("failed to invalidate entry from s3 file cache")?;

        // Step 1 - Write the stream to a tempfile and validate its length.
        let tmp_dir = tempfile::tempdir()?;
        let tmp_path = tmp_dir.path().join(&id);
        let size = betterstreams::fs::write_all(&tmp_path, stream, Some(expected_size)).await?;
        ensure!(
            size == expected_size,
            "stream size and size header were not equal"
        );
        let fstream = betterstreams::fs::read_range(&tmp_path, None).await?.stream;

        // Step 2 - Once we know the file is of valid length, we write it to S3.
        let mp_upload = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&id)
            .send()
            .await
            .context("failed to create s3 multipart upload")?;

        let upload_id = mp_upload
            .upload_id
            .ok_or_else(|| anyhow!("missing upload ID"))?;

        tracing::trace!(id=?upload_id, "created multipart upload");

        match self
            .do_multipart(id.clone(), upload_id.clone(), fstream)
            .await
        {
            Ok((completed_parts, total_length)) => {
                let _ = self
                    .client
                    .complete_multipart_upload()
                    .bucket(self.bucket.clone())
                    .key(id.clone())
                    .upload_id(&upload_id)
                    .multipart_upload(completed_parts)
                    .send()
                    .await
                    .context("failed to complete multipart upload")?;

                tracing::debug!(length = total_length, "completed multipart upload");

                Ok(())
            }
            Err(e) => {
                self.client
                    .abort_multipart_upload()
                    .bucket(&self.bucket)
                    .key(&id)
                    .upload_id(&upload_id)
                    .send()
                    .await
                    .context("failed to abort multipart upload")?;
                bail!("failed upload: {}", e.to_string());
            }
        }
    }

    #[tracing::instrument(name = "s3.write", skip(self, body))]
    async fn write(&self, id: String, range: (Bound<u64>, Bound<u64>), body: Bytes) -> Result<u64> {
        let file_path = self.file_cache.get(&id).await?;
        let range = util::bounds_to_range(range, 0, 0);
        let (start, end) = (range.start, range.end);
        ensure!(
            start < end,
            "invalid range, end bound is smaller than start bound"
        );

        {
            tracing::trace!(path=?file_path, range=?range, length=body.len(), "beginning cache write operation");
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&file_path)
                .await
                .context("failed to open file cache entry")?;
            f.seek(SeekFrom::Start(start)).await?;
            tracing::trace!("seeked to offset {}", start);
            f.write_all(body.as_ref()).await?;
            f.sync_all().await?;
            tracing::trace!("fsync ok");
        }

        let file_length = file_path.metadata()?.len();

        Ok(file_length)
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
                .map(|r| r.map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))),
        );

        Ok(StreamInfo {
            stream: io_stream,
            total_size,
            chunk_size,
        })
    }

    #[tracing::instrument(name = "s3.delete", skip(self))]
    async fn delete(&self, blob_id: &str) -> Result<()> {
        self.file_cache
            .invalidate(blob_id)
            .await
            .context("failed to invalidate file cache entry")?;

        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(blob_id.to_string())
            .send()
            .await
            .context("failed to delete blob on s3")?;

        tracing::debug!("s3 object delete complete");

        Ok(())
    }

    #[tracing::instrument(name = "s3.fsync", skip(self))]
    async fn fsync(&self, id: String) -> Result<()> {
        // FIXME: Trigger fsync asynchronously so it doesn't block the call.
        // FIXME: Trigger fsync periodically for cache keys, and every time on cache eviction.
        if let Some(path) = self.file_cache.contains(&id).await {
            let f = fs::File::open(&path).await?;
            let file_length = path.metadata()?.len();

            tracing::trace!("flushing {} bytes", file_length);

            // TODO: Do this multipart?
            let _result = self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(&id)
                .body(ByteStream::read_from().file(f).build().await?)
                .send()
                .await
                .context("s3 PutObject request failed")?;

            tracing::debug!(file_length = file_length, "complete");
        } else {
            tracing::trace!("nothing to flush");
        }

        Ok(())
    }

    async fn available_space(&self) -> Result<Option<u64>> {
        Ok(None)
    }
}
