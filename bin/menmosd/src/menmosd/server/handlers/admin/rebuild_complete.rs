use apikit::auth::StorageNodeIdentity;
use apikit::reject::{Forbidden, InternalServerError};

use interface::message as msg;

use warp::reply;

use crate::server::context::Context;

pub async fn rebuild_complete(
    identity: StorageNodeIdentity,
    context: Context,
    storage_node_id: String,
) -> Result<reply::Response, warp::Rejection> {
    if identity.id != storage_node_id {
        return Err(Forbidden.into());
    }

    context
        .node
        .rebuild_complete(&storage_node_id)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&msg::MessageResponse {
        message: String::from("OK"),
    }))
}
