use std::sync::Arc;

use apikit::reject::InternalServerError;

use interface::{DirectoryNode, ListStorageNodesResponse};

use warp::reply;

pub async fn list_storage_nodes<N: DirectoryNode>(
    node: Arc<N>,
) -> Result<reply::Response, warp::Rejection> {
    let storage_nodes = node
        .list_storage_nodes()
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&ListStorageNodesResponse {
        storage_nodes,
    }))
}
