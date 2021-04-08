use std::net::SocketAddr;

use anyhow::Result;

use apikit::{
    auth::UserIdentity,
    reject::{BadRequest, InternalServerError},
};

use interface::BlobMetaRequest;

use warp::Reply;

use crate::network::get_storage_node_address;
use crate::server::Context;

/// Parse the blob metadata from a header value.
fn parse_metadata(header_value: String) -> Result<BlobMetaRequest> {
    let json_bytes = base64::decode(header_value.as_bytes())?;
    let meta: BlobMetaRequest = serde_json::from_slice(&json_bytes)?;
    Ok(meta)
}

pub async fn put(
    user: UserIdentity,
    context: Context,
    meta: String,
    addr: Option<SocketAddr>,
) -> Result<warp::reply::Response, warp::Rejection> {
    let socket_addr = addr.ok_or_else(|| InternalServerError::from("missing socket address"))?;

    let meta = parse_metadata(meta).map_err(|_| BadRequest)?;

    // Pick a storage node for our new blob.
    let new_blob_id = uuid::Uuid::new_v4().to_string();
    let targeted_storage_node = context
        .node
        .pick_node_for_blob(&new_blob_id, meta, &user.username)
        .await
        .map_err(InternalServerError::from)?;

    // Redirect the uploader to the node's address.
    let node_address = get_storage_node_address(
        socket_addr.ip(),
        targeted_storage_node,
        &context.config,
        &format!("blob/{}", &new_blob_id),
    )
    .map_err(InternalServerError::from)?;

    log::info!("redirecting to {}", &node_address);

    Ok(warp::redirect::temporary(node_address).into_response())
}
