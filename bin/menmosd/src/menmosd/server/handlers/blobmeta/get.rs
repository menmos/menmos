use apikit::reject::InternalServerError;

use interface::GetMetaResponse;

use warp::reply;

use crate::server::Context;

pub async fn get(context: Context, blob_id: String) -> Result<reply::Response, warp::Rejection> {
    let blob_meta_maybe = context
        .node
        .get_blob_meta(&blob_id)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&GetMetaResponse {
        meta: blob_meta_maybe,
    }))
}
