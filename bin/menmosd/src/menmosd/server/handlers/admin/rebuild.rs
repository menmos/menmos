use apikit::{
    auth::UserIdentity,
    reject::{Forbidden, InternalServerError},
};

use interface::message as msg;

use warp::reply;

use crate::server::context::Context;

pub async fn rebuild(
    user: UserIdentity,
    context: Context,
) -> Result<reply::Response, warp::Rejection> {
    if !user.admin {
        return Err(Forbidden.into());
    }

    context
        .node
        .start_rebuild()
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&msg::MessageResponse {
        message: String::from("Rebuild started"),
    }))
}
