use apikit::reject::InternalServerError;
use menmos_auth::UserIdentity;

use warp::reply;

use crate::server::context::Context;

#[tracing::instrument(skip(context))]
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
