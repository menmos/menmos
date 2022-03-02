use std::sync::Arc;

use apikit::reject::HTTPError;

use axum::extract::{Extension, Path};
use axum::response::Redirect;
use axum_client_ip::ClientIp;

use menmos_auth::UserIdentity;

use interface::DynDirectoryNode;

use crate::network::get_storage_node_address;
use crate::Config;

#[tracing::instrument(skip(node, config, addr))]
pub async fn delete(
    user: UserIdentity,
    Extension(node): Extension<DynDirectoryNode>,
    Extension(config): Extension<Arc<Config>>,
    ClientIp(addr): ClientIp,
    Path(blob_id): Path<String>,
) -> Result<Redirect, HTTPError> {
    let blob_info = node
        .indexer()
        .get_blob_meta(&blob_id, &user.username)
        .await
        .map_err(HTTPError::internal_server_error)?
        .ok_or(HTTPError::NotFound)?;

    if blob_info.owner != user.username {
        return Err(HTTPError::NotFound);
    }

    let storage_node = node
        .indexer()
        .get_blob_storage_node(&blob_id)
        .await
        .map_err(HTTPError::internal_server_error)?
        .ok_or(HTTPError::NotFound)?;

    let node_address =
        get_storage_node_address(addr, storage_node, &config, &format!("blob/{}", &blob_id))
            .map_err(HTTPError::internal_server_error)?;

    Ok(Redirect::temporary(node_address))
}
