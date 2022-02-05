use apikit::reject::{Forbidden, InternalServerError};

use menmos_auth::UserIdentity;

use warp::reply;

use crate::server::Context;

#[tracing::instrument(skip(context))]
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
