use apikit::reject::{HTTPError, InternalServerError};

use axum::extract::Extension;
use axum::Json;

use menmos_auth::UserIdentity;

use interface::DynDirectoryNode;

use protocol::directory::storage::ListStorageNodesResponse;

use crate::server::context::Context;

#[tracing::instrument(skip(node))]
pub async fn list(
    _user: UserIdentity,
    Extension(node): Extension<DynDirectoryNode>,
) -> Result<Json<ListStorageNodesResponse>, HTTPError> {
    let storage_nodes = node
        .admin()
        .list_storage_nodes()
        .await
        .map_err(HTTPError::internal_server_error)?;

    Ok(Json(ListStorageNodesResponse { storage_nodes }))
}
