use std::io::{self, SeekFrom};
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
use tokio::io::{AsyncRead, AsyncWriteExt};
use tokio_util::codec;

use interface::Range;

use super::FileCache;
use crate::{Repository, StreamInfo};

/// Get the total length of a blob from a Content-Range header value.
fn get_total_length(range_string: &str) -> Result<u64> {
    let splitted: Vec<_> = range_string.split('/').collect();
    ensure!(splitted.len() == 2, "invalid range response header");

    let total_size = splitted[1].parse::<u64>()?;

    Ok(total_size)
}

fn into_bytes_stream<R>(r: R) -> impl Stream<Item = Result<Bytes, io::Error>>
where
    R: AsyncRead,
{
    codec::FramedRead::new(r, codec::BytesCodec::new()).map_ok(|bytes| bytes.freeze())
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

    async fn write(&self, id: String, range: interface::Range, body: Bytes) -> Result<u64> {
        let file_path = self.file_cache.get(&id).await?;

        let (start, end) = (
            range.min_value().unwrap_or(0),
            range
                .max_value()
                .map(|v| v + 1) // HTTP ranges are inclusive, byte ranges on disk are exclusive.
                .ok_or_else(|| anyhow!("missing end bound"))?,
        );

        ensure!(start < end, "invalid range");

        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&file_path)
                .await?;
            f.seek(SeekFrom::Start(start)).await?;
            f.write_all(body.as_ref()).await?;
        }

        let f = fs::File::open(&file_path).await?;
        let file_length = file_path.metadata()?.len();
        let _result = self
            .client
            .put_object(PutObjectRequest {
                bucket: self.bucket.clone(),
                key: id,
                body: Some(StreamingBody::new_with_size(
                    into_bytes_stream(f),
                    file_length as usize,
                )),
                ..Default::default()
            })
            .await?;

        Ok(file_length)
    }

    async fn get(&self, blob_id: &str, range: Option<Range>) -> Result<StreamInfo> {
        let mut get_request = GetObjectRequest {
            bucket: self.bucket.clone(),
            key: blob_id.to_string(),
            ..Default::default()
        };

        if let Some(r) = range.as_ref() {
            let fmt_max_value = match r.max_value() {
                Some(m) => format!("{}", m),
                None => String::default(),
            };
            let range_str = format!("bytes={}-{}", r.min_value().unwrap_or(0), fmt_max_value);
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
                total_blob_size: total_size,
                current_chunk_size: chunk_size,
            })
        } else {
            Err(anyhow!("missing stream"))
        }
    }

    async fn delete(&self, blob_id: &str) -> Result<()> {
        let delete_request = DeleteObjectRequest {
            bucket: self.bucket.clone(),
            key: blob_id.to_string(),
            ..Default::default()
        };

        self.client.delete_object(delete_request).await?;

        Ok(())
    }
}
