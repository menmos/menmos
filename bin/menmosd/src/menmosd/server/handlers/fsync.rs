use std::net::SocketAddr;
use std::sync::Arc;

use apikit::reject::{InternalServerError, NotFound};

use interface::DirectoryNode;

use warp::Reply;

use crate::network::get_storage_node_address;
use crate::Config;

pub async fn fsync<N: DirectoryNode>(
    cfg: Config,
    node: Arc<N>,
    blob_id: String,
    addr: Option<SocketAddr>,
) -> Result<warp::reply::Response, warp::Rejection> {
    let socket_addr = addr.ok_or_else(|| InternalServerError::from("missing socket address"))?;

    let storage_node = node
        .get_blob_storage_node(&blob_id)
        .await
        .map_err(InternalServerError::from)?
        .ok_or(NotFound)?;

    let node_address = get_storage_node_address(
        socket_addr.ip(),
        storage_node,
        &cfg,
        &format!("blob/{}/fsync", &blob_id),
    )
    .map_err(InternalServerError::from)?;

    Ok(warp::redirect::temporary(node_address).into_response())
}