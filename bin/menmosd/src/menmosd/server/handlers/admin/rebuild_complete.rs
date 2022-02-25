use apikit::reject::{Forbidden, HTTPError, InternalServerError};
use axum::extract::{Extension, Path};
use axum::Json;
use menmos_auth::StorageNodeIdentity;

use apikit::payload::MessageResponse;
use interface::DynDirectoryNode;
use warp::reply;

use crate::server::Context;

#[tracing::instrument(skip(node))]
pub async fn rebuild_complete(
    identity: StorageNodeIdentity,
    Extension(node): Extension<DynDirectoryNode>,
    Path(storage_node_id): Path<String>,
) -> Result<Json<MessageResponse>, HTTPError> {
    if identity.id != storage_node_id {
        return Err(HTTPError::Forbidden);
    }

    node.admin()
        .rebuild_complete(&storage_node_id)
        .await
        .map_err(HTTPError::internal_server_error)?;

    Ok(Json(MessageResponse::new("OK")))
}
