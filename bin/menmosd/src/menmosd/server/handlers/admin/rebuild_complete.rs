use apikit::reject::HTTPError;

use axum::extract::{Extension, Path};
use axum::response::Response;

use interface::DynDirectoryNode;

use menmos_auth::StorageNodeIdentity;

#[tracing::instrument(name = "handler.admin.rebuild_complete", skip(node))]
pub async fn rebuild_complete(
    identity: StorageNodeIdentity,
    Extension(node): Extension<DynDirectoryNode>,
    Path(storage_node_id): Path<String>,
) -> Result<Response, HTTPError> {
    if identity.id != storage_node_id {
        return Err(HTTPError::Forbidden);
    }

    node.admin()
        .rebuild_complete(&storage_node_id)
        .await
        .map_err(HTTPError::internal_server_error)?;

    Ok(apikit::reply::message("ok"))
}
