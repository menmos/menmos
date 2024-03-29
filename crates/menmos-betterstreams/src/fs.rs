//! Filesystem stream utilities.

use std::cmp;
use std::io::{self, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::Poll;

use anyhow::{ensure, Result};
use bytes::BytesMut;
use futures::{prelude::*, ready};

use tokio::fs;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio_util::io::poll_read_buf;

use crate::{ChunkedStreamInfo, UnpinDynIoStream};

const DEFAULT_READ_BUF_SIZE: usize = 8_192;

/// Consumes a stream while writing it to a file.
///
/// # Examples
/// ```no_run
/// use betterstreams::{fs, UnpinDynIoStream};
/// use tokio::runtime::Runtime;
///
/// Runtime::new().unwrap().block_on(async {
///     let mystream: UnpinDynIoStream = { unimplemented!(); };
///     fs::write_all("/home/user/myfile.txt", mystream, None).await.unwrap();
/// });
/// ```
pub async fn write_all<P: AsRef<Path>>(
    path: P,
    stream: UnpinDynIoStream,
    max_size: Option<u64>,
) -> Result<u64> {
    let mut stream_pin = Box::pin(stream);

    let mut f = fs::File::create(path.as_ref()).await?;

    let mut total_size: u64 = 0;
    while let Some(chunk) = stream_pin.next().await {
        let chunk_bytes = chunk?;
        total_size += chunk_bytes.len() as u64;

        if let Some(max) = max_size {
            ensure!(
                total_size <= max,
                "early stop: stream is bigger than expected"
            );
        }

        f.write_all(chunk_bytes.as_ref()).await?;
    }

    f.flush().await?;

    Ok(f.metadata().await?.len())
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

async fn seek_file(mut f: fs::File, offset: u64) -> io::Result<fs::File> {
    if offset > 0 {
        f.seek(SeekFrom::Start(offset)).await?;
    }
    Ok(f)
}

fn reserve_at_least(buf: &mut BytesMut, cap: usize) {
    if buf.capacity() - buf.len() < cap {
        buf.reserve(cap);
    }
}

/// Read a range of bytes from a file.
///
/// Passing a `None` range will return the whole file as a stream.
///
/// # Examples
/// ```no_run
/// use std::ops::Range;
///
/// use betterstreams::fs;
/// use tokio::runtime::Runtime;
///
/// Runtime::new().unwrap().block_on(async {
///     let stream_info = fs::read_range("hello.txt", Some(Range{start: 10, end: 15}))
///         .await.unwrap();
/// })
/// ```
pub async fn read_range<P: AsRef<Path>>(
    path: P,
    range: Option<Range<u64>>,
) -> Result<ChunkedStreamInfo> {
    let file_path = PathBuf::from(path.as_ref());

    let meta = file_path.metadata()?;
    let len_total = meta.len();
    let buf_size = optimal_buf_size(&meta);

    let (start, end) = range
        .map(|r| (r.start, r.end.min(len_total)))
        .unwrap_or((0, len_total));

    ensure!((start < end) || (start == end && end == 0), "invalid range");
    ensure!(end <= len_total, "range too long");

    let mut len = end - start;

    let seek = fs::File::open(file_path).and_then(move |f| seek_file(f, start));

    let s = seek
        .into_stream()
        .map(move |result| {
            let mut buf = BytesMut::new();
            let mut f = match result {
                Ok(f) => f,
                Err(f) => {
                    tracing::error!("unexpected state in stream: {}", f);
                    panic!("find out why this is reached");
                }
            };

            stream::poll_fn(move |cx| {
                if len == 0 {
                    return Poll::Ready(None);
                }

                reserve_at_least(&mut buf, buf_size);

                let n = match ready!(poll_read_buf(Pin::new(&mut f), cx, &mut buf)) {
                    Ok(n) => n as u64,
                    Err(err) => {
                        tracing::debug!("file read error: {}", err);
                        return Poll::Ready(Some(Err(err)));
                    }
                };

                if n == 0 {
                    tracing::debug!("file read found EOF before expected length");
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

    Ok(ChunkedStreamInfo {
        stream: Box::from(s),
        chunk_size: len,
        total_size: len_total,
    })
}
