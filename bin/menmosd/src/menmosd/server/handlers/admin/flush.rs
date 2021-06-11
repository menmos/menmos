use apikit::{
    auth::UserIdentity,
    reject::{Forbidden, InternalServerError},
};

use warp::reply;

use crate::server::Context;

pub async fn flush(
    user: UserIdentity,
    context: Context,
) -> Result<reply::Response, warp::Rejection> {
    if !user.admin {
        return Err(Forbidden.into());
    }

    context
        .node
        .flush()
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::message("OK"))
}
