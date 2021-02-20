use apikit::{auth::UserIdentity, reject::InternalServerError};

use protocol::directory::blobmeta::ListMetadataRequest;

use warp::reply;

use crate::server::Context;

pub async fn list(
    _user: UserIdentity,
    context: Context,
    req: ListMetadataRequest,
) -> Result<reply::Response, warp::Rejection> {
    let response = context
        .node
        .list_metadata(req.tags, req.meta_keys)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&response))
}
