use std::sync::Arc;

use apikit::{
    auth::UserIdentity,
    reject::{Forbidden, InternalServerError},
};

use interface::StorageNode;

use warp::reply;

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
