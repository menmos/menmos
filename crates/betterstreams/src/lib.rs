//! Library to make working with streams a bit less painful.
use std::io;

use bytes::Bytes;
use futures::Stream;

pub mod fs;
pub mod util;

/// A boxed stream of [`Bytes`] that can be unpinned.
///
/// [`Bytes`]: bytes::Bytes
pub type UnpinDynIOStream =
    Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin + 'static>;

/// A boxed stream of [`Bytes`].
///
/// [`Bytes`]: bytes::Bytes
pub type DynIOStream = Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + 'static>;

/// A stream that can be part of a larger file.
pub struct ChunkedStreamInfo {
    /// The stream data.
    pub stream: DynIOStream,

    /// The size of the current stream chunk, in bytes.
    pub chunk_size: u64,

    /// The total size of the file the stream was extracted from, in bytes.
    pub total_size: u64,
}
