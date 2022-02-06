use apikit::auth::UserIdentity;
use apikit::reject::Forbidden;

use crate::server::Context;

#[tracing::instrument(skip(context))]
pub async fn get_config(
    user: UserIdentity,
    context: Context,
) -> Result<impl warp::Reply, warp::Rejection> {
    if !user.admin {
        return Err(Forbidden.into());
    }
    Ok(apikit::reply::json(&*context.config))
}
