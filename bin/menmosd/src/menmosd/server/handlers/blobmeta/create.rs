use apikit::reject::HTTPError;

use axum::extract::{Extension, Path};
use axum::response::Response;
use axum::Json;

use interface::{BlobInfo, DynDirectoryNode};

use menmos_auth::StorageNodeIdentity;

#[tracing::instrument("handler.meta.create", skip(node, blob_info))]
pub async fn create(
    identity: StorageNodeIdentity,
    Path(blob_id): Path<String>,
    Extension(node): Extension<DynDirectoryNode>,
    Json(blob_info): Json<BlobInfo>,
) -> Result<Response, HTTPError> {
    node.indexer()
        .index_blob(&blob_id, blob_info, &identity.id)
        .await
        .map_err(HTTPError::internal_server_error)?;

    Ok(apikit::reply::message("ok"))
}
