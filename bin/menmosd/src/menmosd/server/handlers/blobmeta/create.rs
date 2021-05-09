use apikit::auth::StorageNodeIdentity;
use apikit::reject::InternalServerError;

use interface::BlobInfo;

use warp::reply;

use crate::server::Context;

pub async fn create(
    identity: StorageNodeIdentity,
    context: Context,
    blob_id: String,
    blob_info: BlobInfo,
) -> Result<reply::Response, warp::Rejection> {
    context
        .node
        .indexer()
        .index_blob(&blob_id, blob_info, &identity.id)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::message("Ok"))
}
