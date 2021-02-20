use apikit::{auth::UserIdentity, reject::InternalServerError};

use interface::ListStorageNodesResponse;

use warp::reply;

use crate::server::context::Context;

pub async fn list(
    _user: UserIdentity,
    context: Context,
) -> Result<reply::Response, warp::Rejection> {
    let storage_nodes = context
        .node
        .list_storage_nodes()
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&ListStorageNodesResponse {
        storage_nodes,
    }))
}
