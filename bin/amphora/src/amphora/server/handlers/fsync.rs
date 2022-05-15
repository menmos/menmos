use apikit::reject::HTTPError;

use axum::extract::{Extension, Path};
use axum::response::Response;

use interface::DynStorageNode;

use menmos_auth::UserIdentity;

#[tracing::instrument(name = "handle.fsync", skip(node))]
pub async fn fsync(
    user: UserIdentity,
    Extension(node): Extension<DynStorageNode>,
    Path(blob_id): Path<String>,
) -> Result<Response, HTTPError> {
    node.fsync(blob_id, &user.username)
        .await
        .map_err(HTTPError::internal_server_error)?;
    Ok(apikit::reply::message("ok"))
}
