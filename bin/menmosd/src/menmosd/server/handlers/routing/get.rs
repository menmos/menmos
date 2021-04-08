use apikit::auth::UserIdentity;
use apikit::reject::InternalServerError;

use protocol::directory::routing::GetRoutingConfigResponse;
use warp::reply;

use crate::server::context::Context;

pub async fn get(user: UserIdentity, context: Context) -> Result<reply::Response, warp::Rejection> {
    let routing_config = context
        .node
        .get_routing_config(&user.username)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&GetRoutingConfigResponse {
        routing_config,
    }))
}
