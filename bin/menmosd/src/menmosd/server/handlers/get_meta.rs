use std::sync::Arc;

use apikit::reject::InternalServerError;

use interface::{DirectoryNode, GetMetaResponse};

use warp::reply;

pub async fn get_meta<N: DirectoryNode>(
    node: Arc<N>,
    blob_id: String,
) -> Result<reply::Response, warp::Rejection> {
    let blob_meta_maybe = node
        .get_blob_meta(&blob_id)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&GetMetaResponse {
        meta: blob_meta_maybe,
    }))
}
