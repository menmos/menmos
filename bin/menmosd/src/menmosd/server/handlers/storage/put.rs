use std::net::SocketAddr;

use anyhow::Result;

use apikit::reject::{Forbidden, InternalServerError};

use interface::{MoveInformation, StorageNodeInfo};

use protocol::directory::storage::{MoveRequest, RegisterResponse};

use warp::reply;

use crate::network::get_storage_node_address;
use crate::server::context::Context;

const MESSAGE_REGISTRATION_SUCCESSFUL: &str = "storage node registered";

fn get_request_from_move_info(
    move_info: MoveInformation,
    socket_addr: &SocketAddr,
    context: &Context,
) -> Result<MoveRequest> {
    let node_address = get_storage_node_address(
        socket_addr.ip(),
        move_info.destination_node,
        &context.config,
        &format!("blob/{}", &move_info.blob_id),
    )?;

    Ok(MoveRequest {
        blob_id: move_info.blob_id,
        owner_username: move_info.owner_username,
        destination_url: node_address.to_string(),
    })
}

#[tracing::instrument(skip(context, addr, info))]
pub async fn put(
    identity: menmos_auth::StorageNodeIdentity,
    context: Context,
    info: StorageNodeInfo,
    addr: Option<SocketAddr>,
) -> Result<reply::Response, warp::Rejection> {
    if identity.id != info.id {
        return Err(Forbidden.into());
    }

    let socket_addr = addr.ok_or_else(|| InternalServerError::from("missing socket address"))?;

    let node_resp = context
        .node
        .admin()
        .register_storage_node(info)
        .await
        .map_err(InternalServerError::from)?;

    let certificates = (*context.certificate_info).clone();

    let move_requests: Vec<MoveRequest> = context
        .node
        .routing()
        .get_move_requests(&identity.id)
        .await
        .map_err(InternalServerError::from)?
        .into_iter()
        .map(|info| get_request_from_move_info(info, &socket_addr, &context))
        .collect::<Result<Vec<MoveRequest>>>()
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&RegisterResponse {
        message: MESSAGE_REGISTRATION_SUCCESSFUL.to_string(),
        certificates,
        rebuild_requested: node_resp.rebuild_requested,
        move_requests,
    }))
}
