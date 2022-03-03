use axum::extract::{Extension, Path};
use axum::Json;

use apikit::payload::MessageResponse;
use apikit::reject::HTTPError;

use interface::{BlobInfoRequest, BlobMetaRequest, DynStorageNode, StorageNode};

use menmos_auth::UserIdentity;

#[tracing::instrument(skip(node, meta_request))]
pub async fn update_meta(
    user: UserIdentity,
    Extension(node): Extension<DynStorageNode>,
    Path(blob_id): Path<String>,
    Json(meta_request): Json<BlobMetaRequest>,
) -> Result<Json<MessageResponse>, HTTPError> {
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

    Ok(Json(MessageResponse::new("ok")))
}
