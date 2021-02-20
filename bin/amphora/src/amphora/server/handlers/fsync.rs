use std::sync::Arc;

use apikit::{auth::UserIdentity, reject::InternalServerError};

use interface::StorageNode;

use warp::reply;

pub async fn fsync<N: StorageNode>(
    _user: UserIdentity,
    node: Arc<N>,
    blob_id: String,
) -> Result<reply::Response, warp::Rejection> {
    node.fsync(blob_id)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::message("OK"))
}
