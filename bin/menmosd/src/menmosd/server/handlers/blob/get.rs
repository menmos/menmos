use std::net::SocketAddr;

use apikit::reject::{InternalServerError, NotFound};

use menmos_auth::UserIdentity;

use warp::{reply, Reply};

use crate::network::get_storage_node_address;
use crate::server::Context;

#[tracing::instrument(skip(context, addr))]
pub async fn get(
    _user: UserIdentity,
    context: Context,
    addr: Option<SocketAddr>,
    blob_id: String,
) -> Result<reply::Response, warp::Rejection> {
    let socket_addr = addr.ok_or_else(|| InternalServerError::from("missing socket address"))?;

    let storage_node = context
        .node
        .indexer()
        .get_blob_storage_node(&blob_id)
        .await
        .map_err(InternalServerError::from)?
        .ok_or(NotFound)?;

    let node_address = get_storage_node_address(
        socket_addr.ip(),
        storage_node,
        &context.config,
        &format!("blob/{}", &blob_id),
    )
    .map_err(InternalServerError::from)?;

    Ok(warp::redirect::temporary(node_address).into_response())
}
