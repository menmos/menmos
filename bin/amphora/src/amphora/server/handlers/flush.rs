use axum::extract::Extension;
use axum::Json;

use apikit::reject::HTTPError;

use interface::{DynStorageNode, StorageNode};

use menmos_auth::UserIdentity;

use apikit::payload::MessageResponse;

#[tracing::instrument(skip(node))]
pub async fn flush(
    user: UserIdentity,
    Extension(node): Extension<DynStorageNode>,
) -> Result<Json<MessageResponse>, HTTPError> {
    if !user.admin {
        return Err(HTTPError::Forbidden);
    }

    node.flush()
        .await
        .map_err(HTTPError::internal_server_error)?;

    Ok(Json(MessageResponse::new("ok")))
}
