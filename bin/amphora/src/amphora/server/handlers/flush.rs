use std::sync::Arc;

use apikit::reject::{Forbidden, InternalServerError};

use interface::StorageNode;

use menmos_auth::UserIdentity;

use warp::reply;

#[tracing::instrument(skip(node))]
pub async fn flush<N: StorageNode>(
    user: UserIdentity,
    node: Arc<N>,
) -> Result<reply::Response, warp::Rejection> {
    if !user.admin {
        return Err(Forbidden.into());
    }

    node.flush().await.map_err(InternalServerError::from)?;

    Ok(apikit::reply::message("OK"))
}
