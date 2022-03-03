use anyhow::Result;
use axum::extract::{BodyStream, Extension, Path, TypedHeader};
use axum::Json;
use std::io;

use apikit::reject::HTTPError;

use bytes::Bytes;

use futures::{Stream, StreamExt};

use interface::{BlobInfoRequest, DynStorageNode};

use menmos_auth::UserIdentity;

use protocol::storage::PutResponse;

use protocol::header::{BlobMetaHeader, BlobSizeHeader};

#[tracing::instrument(skip(node, meta_request, body))]
pub async fn put(
    user: UserIdentity,
    Extension(node): Extension<DynStorageNode>,
    Path(blob_id): Path<String>,
    TypedHeader(BlobMetaHeader(meta_request)): TypedHeader<BlobMetaHeader>,
    TypedHeader(BlobSizeHeader(blob_size)): TypedHeader<BlobSizeHeader>,
    body: BodyStream,
) -> Result<Json<PutResponse>, HTTPError> {
    // Convert the stream error to a std::io::Error
    let stream: Option<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin>> =
        if blob_size == 0 {
            None
        } else {
            Some(Box::new(body.map(|r| {
                r.map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
            })))
        };

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
        Ok(_) => Ok(Json(PutResponse { id: blob_id })),
        Err(e) => Err(HTTPError::internal_server_error(e)),
    }
}
