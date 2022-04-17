use std::io::{self, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;

use anyhow::{anyhow, bail, ensure, Context, Result};

use aws_sdk_s3::error::GetObjectErrorKind;
use aws_sdk_s3::model::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::Client;
use aws_smithy_http::result::SdkError;

use bytes::buf::Writer;
use bytes::{BufMut, Bytes, BytesMut};

use futures::prelude::*;

use lfan::preconfig::concurrent::{new_lru_cache, LruCache};

use tokio::fs;
use tokio::io::AsyncWriteExt;

pub struct FileCache {
    bucket: String,
    client: Client,
    file_path_cache: LruCache<String, PathBuf>,
    root_path: PathBuf,
}

impl FileCache {
    pub fn new<P: Into<PathBuf>, B: Into<String>>(
        directory: P,
        max_nb_of_files: usize,
        bucket: B,
        client: Client,
    ) -> Result<Self> {
        let root_path: PathBuf = directory.into();

        if !root_path.exists() {
            std::fs::create_dir_all(&root_path)?;
        }

        let file_path_cache = new_lru_cache(max_nb_of_files);

        Ok(Self {
            bucket: bucket.into(),
            client,
            file_path_cache,
            root_path,
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

    fn get_blob_path(&self, blob_id: &str) -> PathBuf {
        self.root_path.join(blob_id)
    }

    pub async fn contains<S: AsRef<str>>(&self, blob_id: S) -> Option<PathBuf> {
        self.file_path_cache.get(blob_id.as_ref()).await
    }

    #[tracing::instrument(name = "file_cache.invalidate", skip(self))]
    pub async fn invalidate(&self, blob_id: &str) -> Result<()> {
        let file_path = self.root_path.join(blob_id);
        if file_path.exists() {
            fs::remove_file(&file_path).await.with_context(|| {
                format!("failed to remove entry '{file_path:?}' from file cache")
            })?;
            tracing::trace!(path = ?file_path, "file existed and was removed");
        }
        self.file_path_cache.invalidate(blob_id).await;

        Ok(())
    }

    #[tracing::instrument(name = "file_cache.download_blob", skip(self))]
    async fn download_blob(&self, blob_id: &str) -> Result<PathBuf> {
        match self
            .client
            .get_object()
            .bucket(self.bucket.clone())
            .key(blob_id.to_string())
            .send()
            .await
        {
            Ok(result) => {
                let mut bytestream = result.body;

                let file_path = self.get_blob_path(blob_id);
                let mut f = fs::File::create(&file_path).await.with_context(|| {
                    format!("failed to create destination file '{file_path:?}'")
                })?;
                while let Some(chunk) = bytestream.next().await {
                    match chunk {
                        Ok(c) => f
                            .write_all(c.as_ref())
                            .await
                            .context("failed to write stream chunk to file")?,
                        Err(e) => {
                            tracing::warn!(
                                "file cache encountered an error while downlading blob {blob_id}: {e}"
                            );
                            fs::remove_file(&file_path).await?;
                            return Err(e.into());
                        }
                    }
                }
                tracing::debug!("pull successful",);
                Ok(file_path)
            }
            Err(SdkError::ServiceError { err, raw: _ }) => {
                if let GetObjectErrorKind::NoSuchKey(_) = err.kind {
                    // Create the file - empty - in the file cache.
                    let file_path = self.root_path.join(blob_id);
                    fs::File::create(&file_path)
                        .await
                        .context("failed to create empty file")?;
                    Ok(file_path)
                } else {
                    //Rethrow.
                    Err(err.into())
                }
            }
            Err(e) => {
                // Rethrow
                Err(e.into())
            }
        }
    }

    async fn insert_evict(&self, blob_id: &str, blob_path: &Path) -> Result<bool> {
        let (was_inserted, eviction_victim_maybe) = self
            .file_path_cache
            .insert(blob_id.to_string(), blob_path.into())
            .await;

        if let Some(victim) = eviction_victim_maybe {
            // FIXME(MEN-164): Before deleting the blob on cache evict we should check that
            //                 the blob isn't in an un-synced state (e.g. if it was written to since last
            //                 fsync). Failing to do so might discard user modifications.
            //
            //                 The best way to fix this would be to track sync state for every blob in the
            //                 cache. If an eviction candidate is unsynced, it should trigger an immediate
            //                 fsync before being evicted.
            //
            //                 If the cost of synchronous fsyncs is too high (would block the call for
            //                 too long), we could dispatch a tokio task that takes care of the fsync
            //                 and deletes the local entry afterwards. This is trickier to do because
            //                 in the event a new call comes in to mutate the blob during a sync, we'd
            //                 need to cancel the sync.

            //                 Since our target use case (personal distributed filesystem) makes it
            //                 unlikely that there would be multiple large unsynced blobs in the cache at
            //                 once, the easiest path at first would probably be to trigger a synchronous fsync
            //                 directly here and explore asynchronous options if the synchronous path becomes
            //                 a pain.
            fs::remove_file(&victim)
                .await
                .context("failed to remove evicted item")?;
            tracing::trace!(path=?victim, "removed victim from disk");
        }

        Ok(was_inserted)
    }

    #[tracing::instrument(name = "file_cache.get", skip(self))]
    pub async fn get(&self, blob_id: &str) -> Result<PathBuf> {
        if let Some(cache_hit) = self.contains(&blob_id).await {
            tracing::trace!("cache hit");
            return Ok(cache_hit);
        }

        tracing::trace!("cache miss");

        let blob_path = self.download_blob(blob_id.as_ref()).await?;

        let was_inserted = self.insert_evict(blob_id, &blob_path).await?;
        if !was_inserted {
            // The cache failed to keep our path, fail gracefully.
            fs::remove_file(&blob_path)
                .await
                .context("failed to remove cache candidate")?;
            return Err(anyhow!("failed to insert blob in file cache"));
        }

        Ok(blob_path)
    }

    #[tracing::instrument(skip(self, stream))]
    pub async fn put(
        &self,
        blob_id: &str,
        stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin + 'static>,
        expected_size: u64,
    ) -> Result<PathBuf> {
        let blob_path = self.get_blob_path(blob_id);
        let size = betterstreams::fs::write_all(&blob_path, stream, Some(expected_size)).await?;
        ensure!(
            size == expected_size,
            "stream size and size header were not equal"
        );
        Ok(blob_path)
    }

    #[tracing::instrument(skip(self))]
    pub async fn fsync(&self, blob_id: &str) -> Result<()> {
        if let Some(path) = self.contains(blob_id).await {
            let mp_upload = self
                .client
                .create_multipart_upload()
                .bucket(&self.bucket)
                .key(blob_id)
                .send()
                .await
                .context("failed to create s3 multipart upload")?;

            let upload_id = mp_upload
                .upload_id
                .ok_or_else(|| anyhow!("missing upload ID"))?;

            tracing::trace!(id=?upload_id, "created multipart upload");

            let fstream = betterstreams::fs::read_range(&path, None).await?.stream;

            match self
                .do_multipart(String::from(blob_id), upload_id.clone(), fstream)
                .await
            {
                Ok((completed_parts, total_length)) => {
                    let _ = self
                        .client
                        .complete_multipart_upload()
                        .bucket(self.bucket.clone())
                        .key(blob_id)
                        .upload_id(&upload_id)
                        .multipart_upload(completed_parts)
                        .send()
                        .await
                        .context("failed to complete multipart upload")?;

                    tracing::debug!(length = total_length, "completed multipart upload");
                }
                Err(e) => {
                    self.client
                        .abort_multipart_upload()
                        .bucket(&self.bucket)
                        .key(blob_id)
                        .upload_id(&upload_id)
                        .send()
                        .await
                        .context("failed to abort multipart upload")?;
                    bail!("failed upload: {}", e.to_string());
                }
            };
        }

        Ok(())
    }
}
