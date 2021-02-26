use apikit::auth::StorageNodeIdentity;
use apikit::reject::InternalServerError;

use warp::reply;

use crate::server::Context;

pub async fn delete(
    identity: StorageNodeIdentity,
    context: Context,
    blob_id: String,
) -> Result<reply::Response, warp::Rejection> {
    context
        .node
        .delete_blob(&blob_id, &identity.id)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::message("Ok"))
}
