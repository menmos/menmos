use apikit::reject::{HTTPError, InternalServerError};

use axum::extract::Extension;
use axum::Json;

use interface::DynDirectoryNode;

use menmos_auth::UserIdentity;

use apikit::payload::MessageResponse;

use crate::server::context::Context;

#[tracing::instrument(skip(node))]
pub async fn delete(
    user: UserIdentity,
    Extension(node): Extension<DynDirectoryNode>,
) -> Result<Json<MessageResponse>, HTTPError> {
    node.routing()
        .delete_routing_config(&user.username)
        .await
        .map_err(HTTPError::internal_server_error)?;

    Ok(Json(MessageResponse::new("ok")))
}
