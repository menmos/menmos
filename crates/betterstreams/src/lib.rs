use std::io;

use bytes::Bytes;
use futures::Stream;

pub mod fs;
pub mod util;

pub type UnpinDynIOStream =
    Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin + 'static>;

pub type DynIOStream = Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + 'static>;

pub struct ChunkedStreamInfo {
    pub stream: DynIOStream,
    pub chunk_size: u64,
    pub total_size: u64,
}
