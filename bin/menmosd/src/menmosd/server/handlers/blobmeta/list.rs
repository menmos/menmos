use apikit::{auth::UserIdentity, reject::InternalServerError};

use protocol::directory::blobmeta::ListMetadataRequest;

use warp::reply;

use crate::server::Context;

#[tracing::instrument(skip(context, req))]
pub async fn list(
    user: UserIdentity,
    context: Context,
    req: ListMetadataRequest,
) -> Result<reply::Response, warp::Rejection> {
    tracing::trace!(
        tags = %&req.tags.clone().unwrap_or_default().join(","),
        keys = %&req.meta_keys.clone().unwrap_or_default().join(","),
        "list metadata request"
    );
    let response = context
        .node
        .query()
        .list_metadata(req.tags, req.meta_keys, &user.username)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&response))
}
