use apikit::payload::MessageResponse;
use apikit::reject::{Forbidden, HTTPError, InternalServerError};

use axum::extract::Extension;
use axum::Json;

use menmos_auth::UserIdentity;

use interface::DynDirectoryNode;

use crate::server::Context;

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
