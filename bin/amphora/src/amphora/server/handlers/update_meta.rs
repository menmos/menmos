use apikit::reject::HTTPError;

use axum::extract::{Extension, Path};
use axum::response::Response;
use axum::Json;

use interface::{BlobInfoRequest, BlobMetaRequest, DynStorageNode};

use menmos_auth::UserIdentity;

#[tracing::instrument(name = "handler.update_meta", skip(node, meta_request))]
pub async fn update_meta(
    user: UserIdentity,
    Extension(node): Extension<DynStorageNode>,
    Path(blob_id): Path<String>,
    Json(meta_request): Json<BlobMetaRequest>,
) -> Result<Response, HTTPError> {
    node.update_meta(
        blob_id,
        BlobInfoRequest {
            meta_request,
            size: 0,
            owner: user.username,
        },
    )
    .await
    .map_err(HTTPError::internal_server_error)?;
    Ok(apikit::reply::message("ok"))
}
