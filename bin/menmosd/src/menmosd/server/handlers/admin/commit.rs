use apikit::auth::UserIdentity;
use apikit::reject::{Forbidden, InternalServerError};

use warp::reply;

use crate::server::Context;

pub async fn commit(
    user: UserIdentity,
    context: Context,
) -> Result<reply::Response, warp::Rejection> {
    if !user.admin {
        return Err(Forbidden.into());
    }

    context
        .node
        .commit()
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::message("OK"))
}
