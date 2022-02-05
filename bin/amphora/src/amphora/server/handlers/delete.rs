use std::sync::Arc;

use apikit::reject::InternalServerError;

use interface::StorageNode;

use menmos_auth::UserIdentity;

use warp::reply;

#[tracing::instrument(skip(node))]
pub async fn delete<N: StorageNode>(
    user: UserIdentity,
    node: Arc<N>,
    blob_id: String,
) -> Result<reply::Response, warp::Rejection> {
    node.delete(blob_id, &user.username)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::message("OK"))
}
