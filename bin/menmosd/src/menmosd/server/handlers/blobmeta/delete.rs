use apikit::reject::InternalServerError;
use menmos_auth::StorageNodeIdentity;

use warp::reply;

use crate::server::Context;

#[tracing::instrument(skip(context))]
pub async fn delete(
    identity: StorageNodeIdentity,
    context: Context,
    blob_id: String,
) -> Result<reply::Response, warp::Rejection> {
    context
        .node
        .indexer()
        .delete_blob(&blob_id, &identity.id)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::message("Ok"))
}
