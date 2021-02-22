use apikit::reject::{Forbidden, InternalServerError};

use interface::message::directory_node as msg;
use interface::StorageNodeInfo;

use warp::reply;

use crate::server::context::Context;

const MESSAGE_REGISTRATION_SUCCESSFUL: &str = "storage node registered";

pub async fn put(
    identity: apikit::auth::StorageNodeIdentity,
    context: Context,
    info: StorageNodeInfo,
) -> Result<reply::Response, warp::Rejection> {
    if identity.id != info.id {
        return Err(Forbidden.into());
    }

    let node_resp = context
        .node
        .register_storage_node(info)
        .await
        .map_err(InternalServerError::from)?;

    let certificates = (*context.certificate_info).clone();

    Ok(apikit::reply::json(&msg::RegisterResponse {
        message: MESSAGE_REGISTRATION_SUCCESSFUL.to_string(),
        certificates,
        rebuild_requested: node_resp.rebuild_requested,
    }))
}
