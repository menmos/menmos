use apikit::auth::UserIdentity;
use apikit::reject::InternalServerError;

use protocol::directory::routing::GetRoutingKeyResponse;
use warp::reply;

use crate::server::context::Context;

pub async fn get_key(
    user: UserIdentity,
    context: Context,
) -> Result<reply::Response, warp::Rejection> {
    let routing_key = context
        .node
        .get_routing_key(&user.username)
        .await
        .map_err(InternalServerError::from)?;
    Ok(apikit::reply::json(&GetRoutingKeyResponse { routing_key }))
}
