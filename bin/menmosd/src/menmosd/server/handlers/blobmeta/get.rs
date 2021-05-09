use apikit::{auth::UserIdentity, reject::InternalServerError};

use protocol::directory::blobmeta::GetMetaResponse;

use warp::reply;

use crate::server::Context;

pub async fn get(
    user: UserIdentity,
    context: Context,
    blob_id: String,
) -> Result<reply::Response, warp::Rejection> {
    let info_maybe = context
        .node
        .indexer()
        .get_blob_meta(&blob_id, &user.username)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&GetMetaResponse {
        meta: info_maybe.map(|i| i.meta),
    }))
}
