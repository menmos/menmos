use apikit::reject::HTTPError;

use axum::extract::{Extension, Path};
use axum::Json;

use apikit::payload::MessageResponse;

use interface::DynDirectoryNode;

use menmos_auth::StorageNodeIdentity;

#[tracing::instrument(skip(node))]
pub async fn delete(
    identity: StorageNodeIdentity,
    Path(blob_id): Path<String>,
    Extension(node): Extension<DynDirectoryNode>,
) -> Result<Json<MessageResponse>, HTTPError> {
    node.indexer()
        .delete_blob(&blob_id, &identity.id)
        .await
        .map_err(HTTPError::internal_server_error)?;

    Ok(Json(MessageResponse::new("Ok")))
}
