use std::net::SocketAddr;

use apikit::{
    auth::UserIdentity,
    reject::{InternalServerError, NotFound},
};

use interface::BlobMetaRequest;
use warp::Reply;

use crate::network::get_storage_node_address;
use crate::server::Context;

#[tracing::instrument(skip(context, _meta, addr))]
pub async fn update(
    _user: UserIdentity,
    context: Context,
    blob_id: String,
    _meta: BlobMetaRequest,
    addr: Option<SocketAddr>,
) -> Result<warp::reply::Response, warp::Rejection> {
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
        &format!("blob/{}/metadata", &blob_id),
    )
    .map_err(InternalServerError::from)?;

    Ok(warp::redirect::temporary(node_address).into_response())
}
