use apikit::reject::{Forbidden, InternalServerError};

use interface::message::directory_node as msg;
use interface::StorageNodeInfo;

use warp::reply;

use crate::server::context::Context;

const MESSAGE_REGISTRATION_SUCCESSFUL: &str = "storage node registered";

pub async fn put(
    context: Context,
    registration_secret: String,
    info: StorageNodeInfo,
) -> Result<reply::Response, warp::Rejection> {
    if context.config.node.registration_secret != registration_secret {
        return Err(warp::reject::custom(Forbidden));
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
