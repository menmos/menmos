use std::sync::Arc;

use apikit::reject::HTTPError;

use axum::extract::{Extension, Path};
use axum::response::Redirect;
use axum_client_ip::ClientIp;

use interface::DynDirectoryNode;

use menmos_auth::UserIdentity;

use crate::network::get_storage_node_address;
use crate::Config;

#[tracing::instrument("handler.blob.fsync", skip(node, config, addr))]
pub async fn fsync(
    _user: UserIdentity,
    Extension(node): Extension<DynDirectoryNode>,
    Extension(config): Extension<Arc<Config>>,
    Path(blob_id): Path<String>,
    ClientIp(addr): ClientIp,
) -> Result<Redirect, HTTPError> {
    let storage_node = node
        .indexer()
        .get_blob_storage_node(&blob_id)
        .await
        .map_err(HTTPError::internal_server_error)?
        .ok_or(HTTPError::NotFound)?;

    let node_address = get_storage_node_address(
        addr,
        storage_node,
        &config,
        &format!("blob/{}/fsync", &blob_id),
    )
    .map_err(HTTPError::internal_server_error)?;

    Ok(Redirect::temporary(&node_address.to_string()))
}
