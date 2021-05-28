use apikit::auth::UserIdentity;
use apikit::reject::InternalServerError;

use protocol::directory::routing::SetRoutingConfigRequest;

use warp::reply;

use crate::server::context::Context;

pub async fn set(
    user: UserIdentity,
    context: Context,
    request: SetRoutingConfigRequest,
) -> Result<reply::Response, warp::Rejection> {
    context
        .node
        .routing()
        .set_routing_config(&user.username, &request.routing_config)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::message("ok"))
}
