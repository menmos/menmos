use apikit::reject::{HTTPError, InternalServerError};
use axum::extract::{Extension, Path};
use axum::Json;

use menmos_auth::UserIdentity;

use protocol::directory::blobmeta::GetMetaResponse;

use interface::DynDirectoryNode;
use warp::reply;

use crate::server::Context;

#[tracing::instrument(skip(node))]
pub async fn get(
    user: UserIdentity,
    Path(blob_id): Path<String>,
    Extension(node): Extension<DynDirectoryNode>,
) -> Result<Json<GetMetaResponse>, HTTPError> {
    let info_maybe = node
        .indexer()
        .get_blob_meta(&blob_id, &user.username)
        .await
        .map_err(HTTPError::internal_server_error)?;

    Ok(Json(GetMetaResponse {
        meta: info_maybe.map(|i| i.meta),
    }))
}
