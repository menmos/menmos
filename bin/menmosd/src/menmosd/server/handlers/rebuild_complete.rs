use std::sync::Arc;

use apikit::reject::InternalServerError;

use interface::{message as msg, DirectoryNode};

use warp::reply;

pub async fn rebuild_complete<N: DirectoryNode>(
    node: Arc<N>,
    storage_node_id: String,
) -> Result<reply::Response, warp::Rejection> {
    node.rebuild_complete(&storage_node_id)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&msg::MessageResponse {
        message: String::from("OK"),
    }))
}
