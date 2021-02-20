use apikit::{auth::UserIdentity, reject::InternalServerError};

use protocol::directory::blobmeta::GetMetaResponse;

use warp::reply;

use crate::server::Context;

pub async fn get(
    _user: UserIdentity,
    context: Context,
    blob_id: String,
) -> Result<reply::Response, warp::Rejection> {
    let blob_meta_maybe = context
        .node
        .get_blob_meta(&blob_id)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&GetMetaResponse {
        meta: blob_meta_maybe,
    }))
}
