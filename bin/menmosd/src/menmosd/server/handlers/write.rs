use std::net::SocketAddr;
use std::sync::Arc;

use apikit::reject::{InternalServerError, NotFound};

use bytes::Buf;

use futures::Stream;

use interface::DirectoryNode;

use warp::{reply, Reply};

use crate::{network::get_storage_node_address, Config};

pub async fn write<N: DirectoryNode>(
    cfg: Config,
    node: Arc<N>,
    addr: Option<SocketAddr>,
    blob_id: String,
    _body: impl Stream<Item = Result<impl Buf, warp::Error>> + Send + Sync + Unpin + 'static,
) -> Result<reply::Response, warp::Rejection> {
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
        &format!("blob/{}", &blob_id),
    )
    .map_err(InternalServerError::from)?;

    Ok(warp::redirect::temporary(node_address).into_response())
}
