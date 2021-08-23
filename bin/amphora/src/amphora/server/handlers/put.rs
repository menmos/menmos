use std::io;
use std::sync::Arc;

use anyhow::Result;

use apikit::{
    auth::UserIdentity,
    reject::{BadRequest, InternalServerError},
};

use bytes::{Buf, Bytes};

use futures::{Stream, StreamExt, TryStreamExt};

use headers::HeaderValue;

use interface::{BlobInfoRequest, BlobMetaRequest, StorageNode};

use mime::Mime;

use mpart_async::server::MultipartStream;

use protocol::storage::PutResponse;

use warp::reply::Response;

/// Parse the blob metadata from a header value.
fn parse_metadata(header_value: HeaderValue) -> Result<BlobMetaRequest> {
    let json_bytes = base64::decode(header_value.as_bytes())?;
    let meta: BlobMetaRequest = serde_json::from_slice(&json_bytes)?;
    Ok(meta)
}

/// Prepare a generic boxed stream from a mimetype & a warp stream.
fn prepare_stream(
    mimetype: Mime,
    body: impl Stream<Item = Result<impl Buf, warp::Error>> + Send + Sync + Unpin + 'static,
) -> Result<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin>, warp::Rejection>
{
    let boundary = mimetype
        .get_param("boundary")
        .map(|v| v.to_string())
        .ok_or(apikit::reject::BadRequest)?;

    let stream = MultipartStream::new(
        boundary,
        body.map_ok(|mut buf| buf.copy_to_bytes(buf.remaining())),
    )
    .try_flatten();

    let io_stream =
        stream.map(|r| r.map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string())));

    Ok(Box::from(io_stream))
}

#[tracing::instrument(skip(node, mime, meta, body))]
pub async fn put<N: StorageNode>(
    user: UserIdentity,
    node: Arc<N>,
    blob_id: String,
    mime: Option<Mime>,
    meta: HeaderValue,
    body: impl Stream<Item = Result<impl Buf, warp::Error>> + Send + Sync + Unpin + 'static,
) -> Result<Response, warp::Rejection> {
    // Extract the metadata from the blob.
    let meta_request = parse_metadata(meta).map_err(|_| BadRequest)?;

    // Build our generic stream.
    let mut stream = mime.map(|mime| prepare_stream(mime, body)).transpose()?;

    // Directories cannot have streams.
    if meta_request.blob_type == interface::Type::Directory && stream.is_some() {
        return Err(warp::reject::custom(BadRequest));
    } else if meta_request.blob_type == interface::Type::File && stream.is_none() {
        tracing::debug!("setting default empty stream");
        stream = Some(Box::from(futures::stream::empty()))
    }

    match node
        .put(
            blob_id.clone(),
            BlobInfoRequest {
                meta_request,
                owner: user.username,
            },
            stream,
        )
        .await
    {
        Ok(_) => Ok(apikit::reply::json(&PutResponse { id: blob_id })),
        Err(e) => Err(InternalServerError::from(e).into()),
    }
}
