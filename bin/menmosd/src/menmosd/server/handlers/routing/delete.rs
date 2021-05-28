use apikit::auth::UserIdentity;
use apikit::reject::InternalServerError;

use warp::reply;

use crate::server::context::Context;

pub async fn delete(
    user: UserIdentity,
    context: Context,
) -> Result<reply::Response, warp::Rejection> {
    context
        .node
        .routing()
        .delete_routing_config(&user.username)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::message("ok"))
}
