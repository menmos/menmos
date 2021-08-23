use std::sync::Arc;

use interface::StorageNode;

#[tracing::instrument(skip(_node))]
pub async fn health<N: StorageNode>(_node: Arc<N>) -> Result<impl warp::Reply, warp::Rejection> {
    Ok(apikit::reply::message("healthy"))
}
