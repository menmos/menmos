use std::sync::Arc;

use apikit::reject::{Forbidden, InternalServerError};

use interface::message::directory_node as msg;
use interface::{DirectoryNode, StorageNodeInfo};

use warp::reply;

use crate::Config;

const MESSAGE_REGISTRATION_SUCCESSFUL: &str = "storage node registered";

pub async fn register_storage_node<N: DirectoryNode>(
    node: Arc<N>,
    config: Config,
    certificates: Option<msg::CertificateInfo>,
    registration_secret: String,
    info: StorageNodeInfo,
) -> Result<reply::Response, warp::Rejection> {
    if config.node.registration_secret != registration_secret {
        return Err(warp::reject::custom(Forbidden));
    }

    let node_resp = node
        .register_storage_node(info)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&msg::RegisterResponse {
        message: MESSAGE_REGISTRATION_SUCCESSFUL.to_string(),
        certificates,
        rebuild_requested: node_resp.rebuild_requested,
    }))
}
