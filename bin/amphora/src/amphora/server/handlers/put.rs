use std::io;
use std::sync::Arc;

use anyhow::Result;

use apikit::reject::{BadRequest, InternalServerError};

use bytes::{Buf, Bytes};

use futures::{Stream, StreamExt, TryStreamExt};

use headers::HeaderValue;

use interface::{BlobInfoRequest, BlobMetaRequest, StorageNode};

use mime::Mime;

use menmos_auth::UserIdentity;

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
        .ok_or_else(|| apikit::reject::BadRequest::from("missing boundary"))?;

    let stream = MultipartStream::new(
        boundary,
        body.map_ok(|mut buf| buf.copy_to_bytes(buf.remaining())),
    )
    .try_flatten();

    let io_stream =
        stream.map(|r| r.map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string())));

    Ok(Box::new(io_stream))
}

#[tracing::instrument(skip(node, mime, meta, body))]
pub async fn put<N: StorageNode>(
    user: UserIdentity,
    node: Arc<N>,
    blob_id: String,
    mime: Option<Mime>,
    meta: HeaderValue,
    blob_size: Option<u64>,
    body: impl Stream<Item = Result<impl Buf, warp::Error>> + Send + Sync + Unpin + 'static,
) -> Result<Response, warp::Rejection> {
    // Extract the metadata from the blob.
    let meta_request = parse_metadata(meta).map_err(BadRequest::from)?;

    // Build our generic stream.
    let mut stream = mime.map(|mime| prepare_stream(mime, body)).transpose()?;

    let blob_size = blob_size.unwrap_or_default();

    if blob_size == 0 {
        tracing::debug!("discarding a stream since we received a blob with a size of 0");
        stream = None;
    }

    match node
        .put(
            blob_id.clone(),
            BlobInfoRequest {
                meta_request,
                size: blob_size,
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
