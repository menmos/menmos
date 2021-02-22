use std::sync::Arc;

use interface::StorageNode;

pub async fn health<N: StorageNode>(_node: Arc<N>) -> Result<impl warp::Reply, warp::Rejection> {
    Ok(apikit::reply::message("healthy"))
}
