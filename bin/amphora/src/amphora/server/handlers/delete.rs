use apikit::payload::MessageResponse;
use apikit::reject::HTTPError;

use axum::extract::{Extension, Path};
use axum::Json;

use interface::DynStorageNode;

use menmos_auth::UserIdentity;

#[tracing::instrument(skip(node))]
pub async fn delete(
    user: UserIdentity,
    Extension(node): Extension<DynStorageNode>,
    Path(blob_id): Path<String>,
) -> Result<Json<MessageResponse>, HTTPError> {
    node.delete(blob_id, &user.username)
        .await
        .map_err(HTTPError::internal_server_error)?;
    Ok(Json(MessageResponse::new("ok")))
}
