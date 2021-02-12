use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;

use apikit::reject::{BadRequest, InternalServerError};

use bytes::Buf;

use futures::Stream;

use interface::{BlobMeta, DirectoryNode};
use warp::Reply;

use crate::{network::get_storage_node_address, Config};

/// Parse the blob metadata from a header value.
fn parse_metadata(header_value: String) -> Result<BlobMeta> {
    let json_bytes = base64::decode(header_value.as_bytes())?;
    let meta: BlobMeta = serde_json::from_slice(&json_bytes)?;
    Ok(meta)
}

pub async fn put<N: DirectoryNode>(
    cfg: Config,
    node: Arc<N>,
    meta: String,
    _body: impl Stream<Item = Result<impl Buf, warp::Error>> + Send + Sync + Unpin + 'static,
    addr: Option<SocketAddr>,
) -> Result<warp::reply::Response, warp::Rejection> {
    let socket_addr = addr.ok_or_else(|| InternalServerError::from("missing socket address"))?;

    let meta = parse_metadata(meta).map_err(|_| BadRequest)?;

    // Pick a storage node for our new blob.
    let new_blob_id = uuid::Uuid::new_v4().to_string();
    let targeted_storage_node = node
        .add_blob(&new_blob_id, meta)
        .await
        .map_err(InternalServerError::from)?;

    // Redirect the uploader to the node's address.
    let node_address = get_storage_node_address(
        socket_addr.ip(),
        targeted_storage_node,
        &cfg,
        &format!("blob/{}", &new_blob_id),
    )
    .map_err(InternalServerError::from)?;

    log::info!("redirecting to {}", &node_address);

    Ok(warp::redirect::temporary(node_address).into_response())
}
