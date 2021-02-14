use std::sync::Arc;

use apikit::reject::InternalServerError;

use interface::{DirectoryNode, ListMetadataRequest};

use warp::reply;

pub async fn list_metadata<N: DirectoryNode>(
    node: Arc<N>,
    req: ListMetadataRequest,
) -> Result<reply::Response, warp::Rejection> {
    let response = node
        .list_metadata(&req)
        .await
        .map_err(InternalServerError::from)?;
    Ok(apikit::reply::json(&response))
}
