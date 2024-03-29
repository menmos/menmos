use apikit::reject::HTTPError;

use axum::extract::Extension;
use axum::Json;

use menmos_auth::UserIdentity;

use interface::DynDirectoryNode;

use protocol::directory::storage::ListStorageNodesResponse;

#[tracing::instrument("handler.storage.list", skip(node))]
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
