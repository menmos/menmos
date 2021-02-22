use std::net::SocketAddr;

use apikit::auth::UserIdentity;
use apikit::reject::InternalServerError;

use warp::{reply, Reply};

use crate::network::get_storage_node_address;
use crate::server::Context;

pub async fn delete(
    user: UserIdentity,
    context: Context,
    addr: Option<SocketAddr>,
    blob_id: String,
) -> Result<reply::Response, warp::Rejection> {
    let socket_addr = addr.ok_or_else(|| InternalServerError::from("missing socket address"))?;

    let storage_node = context
        .node
        .delete_blob(&blob_id, &user.username)
        .await
        .map_err(InternalServerError::from)?;

    if let Some(node_info) = storage_node {
        // We want to redirect to the storage node so that it can delete the blob as well.
        let node_address = get_storage_node_address(
            socket_addr.ip(),
            node_info,
            &context.config,
            &format!("blob/{}", &blob_id),
        )
        .map_err(InternalServerError::from)?;

        log::debug!("redirecting to: {}", node_address);
        Ok(warp::redirect::temporary(node_address).into_response())
    } else {
        Ok(apikit::reply::message("OK"))
    }
}
