use apikit::payload::MessageResponse;
use apikit::reject::HTTPError;

use axum::extract::Extension;
use axum::Json;

use interface::DynDirectoryNode;

use menmos_auth::UserIdentity;

#[tracing::instrument(skip(node))]
pub async fn flush(
    user: UserIdentity,
    Extension(node): Extension<DynDirectoryNode>,
) -> Result<Json<MessageResponse>, HTTPError> {
    if !user.admin {
        return Err(HTTPError::Forbidden);
    }

    node.flush()
        .await
        .map_err(HTTPError::internal_server_error)?;

    Ok(Json(MessageResponse::new("OK")))
}
