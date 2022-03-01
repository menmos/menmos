use std::net::IpAddr;
use std::sync::Arc;

use anyhow::Result;

use axum::extract::Extension;
use axum::Json;
use axum_client_ip::ClientIp;

use apikit::reject::HTTPError;

use interface::{CertificateInfo, DynDirectoryNode, MoveInformation, StorageNodeInfo};

use menmos_auth::StorageNodeIdentity;

use protocol::directory::storage::{MoveRequest, RegisterResponse};

use crate::network::get_storage_node_address;
use crate::Config;

const MESSAGE_REGISTRATION_SUCCESSFUL: &str = "storage node registered";

fn get_request_from_move_info(
    move_info: MoveInformation,
    ip_addr: IpAddr,
    config: &Config,
) -> Result<MoveRequest> {
    let node_address = get_storage_node_address(
        ip_addr,
        move_info.destination_node,
        config,
        &format!("blob/{}", &move_info.blob_id),
    )?;

    Ok(MoveRequest {
        blob_id: move_info.blob_id,
        owner_username: move_info.owner_username,
        destination_url: node_address.to_string(),
    })
}

#[tracing::instrument(skip(config, node, addr, info, certificate_info))]
pub async fn put(
    identity: StorageNodeIdentity,
    Extension(certificate_info): Extension<Arc<Option<CertificateInfo>>>,
    Extension(config): Extension<Arc<Config>>,
    Extension(node): Extension<DynDirectoryNode>,
    ClientIp(addr): ClientIp,
    Json(info): Json<StorageNodeInfo>,
) -> Result<Json<RegisterResponse>, HTTPError> {
    if identity.id != info.id {
        return Err(HTTPError::Forbidden);
    }

    let node_resp = node
        .admin()
        .register_storage_node(info)
        .await
        .map_err(HTTPError::internal_server_error)?;

    let certificates = (*certificate_info).clone();

    let move_requests: Vec<MoveRequest> = node
        .routing()
        .get_move_requests(&identity.id)
        .await
        .map_err(HTTPError::internal_server_error)?
        .into_iter()
        .map(|info| get_request_from_move_info(info, addr, &config))
        .collect::<Result<Vec<MoveRequest>>>()
        .map_err(HTTPError::internal_server_error)?;

    Ok(Json(RegisterResponse {
        message: MESSAGE_REGISTRATION_SUCCESSFUL.to_string(),
        certificates,
        rebuild_requested: node_resp.rebuild_requested,
        move_requests,
    }))
}
