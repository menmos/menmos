use std::sync::Arc;

use interface::message as msg;
use interface::DirectoryNode;

use warp::reply;

pub async fn health<N: DirectoryNode>(_node: Arc<N>) -> Result<impl warp::Reply, warp::Rejection> {
    Ok(reply::json(&msg::MessageResponse {
        message: String::from("healthy"),
    }))
}
