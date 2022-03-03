use apikit::payload::MessageResponse;

use axum::extract::{Extension, Path};
use axum::Json;

use apikit::reject::{HTTPError, InternalServerError};

use interface::{DynStorageNode, StorageNode};

use menmos_auth::UserIdentity;

#[tracing::instrument(skip(node))]
pub async fn fsync(
    user: UserIdentity,
    Extension(node): Extension<DynStorageNode>,
    Path(blob_id): Path<String>,
) -> Result<Json<MessageResponse>, HTTPError> {
    node.fsync(blob_id, &user.username)
        .await
        .map_err(HTTPError::internal_server_error)?;
    Ok(Json(MessageResponse::new("ok")))
}
