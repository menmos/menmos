use std::sync::Arc;

use interface::{message::storage_node as msg, StorageNode};

use warp::reply;

pub async fn health<N: StorageNode>(_node: Arc<N>) -> Result<impl warp::Reply, warp::Rejection> {
    Ok(reply::json(&msg::MessageResponse {
        message: String::from("healthy"),
    }))
}
