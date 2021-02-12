use std::net::SocketAddr;
use std::sync::Arc;

use apikit::reject::InternalServerError;

use interface::message as msg;
use interface::DirectoryNode;

use warp::{reply, Reply};

use crate::network::get_storage_node_address;
use crate::Config;

pub async fn delete<N: DirectoryNode>(
    cfg: Config,
    node: Arc<N>,
    addr: Option<SocketAddr>,
    blob_id: String,
) -> Result<reply::Response, warp::Rejection> {
    let socket_addr = addr.ok_or_else(|| InternalServerError::from("missing socket address"))?;

    let storage_node = node
        .delete_blob(&blob_id)
        .await
        .map_err(InternalServerError::from)?;

    if let Some(node_info) = storage_node {
        // We want to redirect to the storage node so that it can delete the blob as well.
        let node_address = get_storage_node_address(
            socket_addr.ip(),
            node_info,
            &cfg,
            &format!("blob/{}", &blob_id),
        )
        .map_err(InternalServerError::from)?;

        log::info!("redirecting to: {}", node_address);
        Ok(warp::redirect::temporary(node_address).into_response())
    } else {
        Ok(apikit::reply::json(&msg::MessageResponse {
            message: String::from("OK"),
        }))
    }
}
