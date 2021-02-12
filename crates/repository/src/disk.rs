use std::cmp;
use std::io::{self, SeekFrom};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::Poll;

use anyhow::{anyhow, ensure, Result};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{prelude::*, ready};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncRead, AsyncWriteExt};

use interface::Range;

use super::iface::{Repository, StreamInfo};

const DEFAULT_READ_BUF_SIZE: usize = 8_192;

fn reserve_at_least(buf: &mut BytesMut, cap: usize) {
    if buf.capacity() - buf.len() < cap {
        buf.reserve(cap);
    }
}

fn optimal_buf_size(metadata: &std::fs::Metadata) -> usize {
    let block_size = get_block_size(metadata);
    cmp::min(block_size as u64, metadata.len()) as usize
}

#[cfg(unix)]
fn get_block_size(metadata: &std::fs::Metadata) -> usize {
    use std::os::unix::fs::MetadataExt;
    cmp::max(metadata.blksize() as usize, DEFAULT_READ_BUF_SIZE)
}

#[cfg(not(unix))]
fn get_block_size(_metadata: &std::fs::Metadata) -> usize {
    DEFAULT_READ_BUF_SIZE
}

/// Represents a blob repository stored on disk.
pub struct DiskRepository {
    path: PathBuf,
}

impl DiskRepository {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let p = PathBuf::from(path.as_ref());

        if !p.exists() {
            std::fs::create_dir(path)?;
        }

        ensure!(p.is_dir(), "Path is not a directory");

        Ok(Self { path: p })
    }

    async fn seek_file(mut f: fs::File, offset: u64) -> io::Result<fs::File> {
        if offset > 0 {
            f.seek(SeekFrom::Start(offset)).await?;
        }
        Ok(f)
    }

    fn get_path_for_blob(&self, blob_id: &str) -> PathBuf {
        self.path.join(blob_id.to_string()).with_extension("blob")
    }
}

#[async_trait]
impl Repository for DiskRepository {
    async fn save(
        &self,
        id: String,
        _size: u64,
        stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin + 'static>,
    ) -> Result<()> {
        let mut stream_pin = Box::pin(stream);

        let file_path = self.get_path_for_blob(&id);
        let mut f = fs::File::create(&file_path).await?;

        while let Some(chunk) = stream_pin.next().await {
            match chunk {
                Ok(c) => f.write_all(c.as_ref()).await?,
                Err(e) => {
                    fs::remove_file(&file_path).await?;
                    return Err(anyhow!("{}", e.to_string()));
                }
            }
        }

        Ok(())
    }

    async fn write(&self, id: String, range: interface::Range, body: Bytes) -> Result<u64> {
        let file_path = self.get_path_for_blob(&id);

        let (start, end) = (
            range.min_value().unwrap_or(0),
            range
                .max_value()
                .map(|v| v + 1) // HTTP ranges are inclusive, byte ranges on disk are exclusive.
                .ok_or_else(|| anyhow!("missing end bound"))?,
        );

        ensure!(start < end, "invalid range");

        let old_length = file_path.metadata()?.len();
        let new_length = (start + end).max(old_length);

        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&file_path)
                .await?;
            f.seek(SeekFrom::Start(start)).await?;
            f.write_all(body.as_ref()).await?;
        }

        Ok(new_length)
    }

    async fn get(&self, blob_id: &str, range: Option<Range>) -> Result<StreamInfo> {
        let file_path = self.get_path_for_blob(&blob_id);

        ensure!(
            file_path.exists() && file_path.is_file(),
            "File doesn't exist"
        );

        let meta = file_path.metadata()?;
        let len_total = meta.len();
        let buf_size = optimal_buf_size(&meta);

        let (start, end) = match range {
            Some(r) => (
                r.min_value().unwrap_or(0),
                r.max_value()
                    .map(|v| v + 1) // HTTP ranges are inclusive, byte ranges on disk are exclusive.
                    .unwrap_or(u64::MAX)
                    .min(len_total),
            ),
            None => (0, meta.len()),
        };

        ensure!((start < end) || (start == end && end == 0), "invalid range");
        ensure!(end <= len_total, "range too long");

        let mut len = end - start;

        let seek = fs::File::open(file_path).and_then(move |f| DiskRepository::seek_file(f, start));

        let s = seek
            .into_stream()
            .map(move |result| {
                let mut buf = BytesMut::new();
                let mut f = match result {
                    Ok(f) => f,
                    Err(f) => {
                        log::error!("unexpected state in stream: {}", f);
                        panic!("find out why this is reached");
                    }
                };

                stream::poll_fn(move |cx| {
                    if len == 0 {
                        return Poll::Ready(None);
                    }

                    reserve_at_least(&mut buf, buf_size);

                    let n = match ready!(Pin::new(&mut f).poll_read_buf(cx, &mut buf)) {
                        Ok(n) => n as u64,
                        Err(err) => {
                            log::trace!("file read error: {}", err);
                            return Poll::Ready(Some(Err(err)));
                        }
                    };

                    if n == 0 {
                        log::trace!("file read found EOF before expected length");
                        return Poll::Ready(None);
                    }

                    let mut chunk = buf.split().freeze();
                    if n > len {
                        chunk = chunk.split_to(len as usize);
                        len = 0;
                    } else {
                        len -= n;
                    }

                    Poll::Ready(Some(Ok(chunk)))
                })
            })
            .flatten();

        Ok(StreamInfo {
            stream: Box::from(s),
            current_chunk_size: len,
            total_blob_size: len_total,
        })
    }

    async fn delete(&self, blob_id: &str) -> Result<()> {
        let blob_path = self.get_path_for_blob(blob_id);

        if blob_path.exists() {
            fs::remove_file(&blob_path).await?;
        }

        Ok(())
    }
}
