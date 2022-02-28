use apikit::reject::HTTPError;

use axum::extract::Extension;
use axum::Json;

use menmos_auth::UserIdentity;

use interface::{DynDirectoryNode, MetadataList};

use protocol::directory::blobmeta::ListMetadataRequest;

#[tracing::instrument(skip(node, req))]
pub async fn list(
    user: UserIdentity,
    Extension(node): Extension<DynDirectoryNode>,
    Json(req): Json<ListMetadataRequest>,
) -> Result<Json<MetadataList>, HTTPError> {
    tracing::trace!(
        tags = %&req.tags.clone().unwrap_or_default().join(","),
        keys = %&req.fields.clone().unwrap_or_default().join(","),
        "list metadata request"
    );
    let response = node
        .query()
        .list_metadata(req.tags, req.fields, &user.username)
        .await
        .map_err(HTTPError::internal_server_error)?;

    Ok(Json(response))
}
