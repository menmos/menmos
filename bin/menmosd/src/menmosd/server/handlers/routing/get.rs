use apikit::reject::HTTPError;

use axum::extract::Extension;
use axum::Json;

use interface::DynDirectoryNode;

use menmos_auth::UserIdentity;

use protocol::directory::routing::GetRoutingConfigResponse;

#[tracing::instrument(skip(node))]
pub async fn get(
    user: UserIdentity,
    Extension(node): Extension<DynDirectoryNode>,
) -> Result<Json<GetRoutingConfigResponse>, HTTPError> {
    let routing_config = node
        .routing()
        .get_routing_config(&user.username)
        .await
        .map_err(HTTPError::internal_server_error)?;

    Ok(Json(GetRoutingConfigResponse { routing_config }))
}
