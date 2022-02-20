use apikit::reject::InternalServerError;
use menmos_auth::UserIdentity;

use protocol::directory::routing::GetRoutingConfigResponse;
use warp::reply;

use crate::server::context::Context;

#[tracing::instrument(skip(context))]
pub async fn get(user: UserIdentity, context: Context) -> Result<reply::Response, warp::Rejection> {
    if let Err(e) = context
        .node
        .routing()
        .get_routing_config(&user.username)
        .await
    {
        tracing::error!("error: {}", e);
    }

    let routing_config = context
        .node
        .routing()
        .get_routing_config(&user.username)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&GetRoutingConfigResponse {
        routing_config,
    }))
}
