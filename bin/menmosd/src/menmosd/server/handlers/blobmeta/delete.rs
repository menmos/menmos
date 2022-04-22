use apikit::reject::HTTPError;

use axum::extract::{Extension, Path};
use axum::response::Response;

use interface::DynDirectoryNode;

use menmos_auth::StorageNodeIdentity;

#[tracing::instrument("handler.meta.delete", skip(node))]
pub async fn delete(
    identity: StorageNodeIdentity,
    Path(blob_id): Path<String>,
    Extension(node): Extension<DynDirectoryNode>,
) -> Result<Response, HTTPError> {
    node.indexer()
        .delete_blob(&blob_id, &identity.id)
        .await
        .map_err(HTTPError::internal_server_error)?;

    Ok(apikit::reply::message("ok"))
}
