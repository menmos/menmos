use apikit::auth::UserIdentity;
use apikit::reject::InternalServerError;

use warp::reply;

use crate::server::context::Context;

pub async fn delete_key(
    user: UserIdentity,
    context: Context,
) -> Result<reply::Response, warp::Rejection> {
    context
        .node
        .delete_routing_key(&user.username)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::message("ok"))
}
