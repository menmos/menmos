use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Result};

use apikit::reject::{BadRequest, HTTPError, InternalServerError};

use axum::extract::{Extension, TypedHeader};
use axum::response::Redirect;
use axum_client_ip::ClientIp;

use headers::Error;

use interface::{BlobInfoRequest, BlobMetaRequest, DynDirectoryNode};

use protocol::header::{BlobMetaHeader, BlobSizeHeader};

use menmos_auth::UserIdentity;

use crate::network::get_storage_node_address;
use crate::server::Context;
use crate::Config;

#[tracing::instrument(skip(node, config, meta, addr))]
pub async fn put(
    user: UserIdentity,
    Extension(node): Extension<DynDirectoryNode>,
    Extension(config): Extension<Arc<Config>>,
    TypedHeader(BlobMetaHeader(meta)): TypedHeader<BlobMetaHeader>,
    TypedHeader(BlobSizeHeader(size)): TypedHeader<BlobSizeHeader>,
    ClientIp(addr): ClientIp,
) -> Result<Redirect, HTTPError> {
    // Pick a storage node for our new blob.
    let new_blob_id = uuid::Uuid::new_v4().to_string();

    let blob_info_request = BlobInfoRequest {
        meta_request: meta,
        size,
        owner: user.username,
    };

    let targeted_storage_node = node
        .indexer()
        .pick_node_for_blob(&new_blob_id, blob_info_request)
        .await
        .map_err(HTTPError::internal_server_error)?;

    // Redirect the uploader to the node's address.
    let node_address = get_storage_node_address(
        addr,
        targeted_storage_node,
        &config,
        &format!("blob/{}", &new_blob_id),
    )
    .map_err(HTTPError::internal_server_error)?;

    tracing::debug!("redirecting to {}", &node_address);

    Ok(Redirect::temporary(node_address))
}
