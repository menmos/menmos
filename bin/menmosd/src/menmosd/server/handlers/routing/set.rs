use apikit::auth::UserIdentity;
use apikit::reject::InternalServerError;

use protocol::directory::routing::SetRoutingKeyRequest;

use warp::reply;

use crate::server::context::Context;

pub async fn set_key(
    user: UserIdentity,
    context: Context,
    request: SetRoutingKeyRequest,
) -> Result<reply::Response, warp::Rejection> {
    context
        .node
        .set_routing_key(&user.username, &request.routing_key)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::message("ok"))
}
