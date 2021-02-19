use std::io;

use bytes::Bytes;

use futures::{Stream, TryStreamExt};

use tokio::io::AsyncRead;
use tokio_util::codec;

pub fn reader_to_iostream<R: AsyncRead>(reader: R) -> impl Stream<Item = Result<Bytes, io::Error>> {
    codec::FramedRead::new(reader, codec::BytesCodec::new()).map_ok(|bytes| bytes.freeze())
}
