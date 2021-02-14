use anyhow::Result;
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};

pub async fn stream_to_bytes<
    S: Stream<Item = std::result::Result<Bytes, E>>,
    E: Into<anyhow::Error>,
>(
    stream: S,
) -> Result<Bytes> {
    let buffer_vector = stream
        .map_err(|e| e.into())
        .collect::<Vec<Result<Bytes>>>()
        .await
        .into_iter()
        .collect::<Result<Vec<Bytes>>>()?;
    Ok(buffer_vector
        .into_iter()
        .flat_map(|b| b.into_iter())
        .collect())
}
