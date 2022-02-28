use apikit::payload::MessageResponse;
use apikit::reject::HTTPError;

use axum::extract::Extension;
use axum::Json;

use interface::DynDirectoryNode;

use menmos_auth::UserIdentity;

use protocol::directory::routing::SetRoutingConfigRequest;

#[tracing::instrument(skip(node))]
pub async fn set(
    user: UserIdentity,
    Extension(node): Extension<DynDirectoryNode>,
    request: Json<SetRoutingConfigRequest>,
) -> Result<Json<MessageResponse>, HTTPError> {
    node.routing()
        .set_routing_config(&user.username, &request.routing_config)
        .await
        .map_err(HTTPError::internal_server_error)?;

    Ok(Json(MessageResponse::new("ok")))
}
