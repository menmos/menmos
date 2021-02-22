use apikit::auth::StorageNodeIdentity;
use apikit::reject::InternalServerError;

use interface::message::MessageResponse;
use interface::BlobMeta;

use warp::reply;

use crate::server::Context;

pub async fn create(
    identity: StorageNodeIdentity,
    context: Context,
    blob_id: String,
    blob_meta: BlobMeta,
) -> Result<reply::Response, warp::Rejection> {
    context
        .node
        .index_blob(&blob_id, blob_meta, &identity.id)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&MessageResponse {
        message: "Ok".to_string(),
    }))
}
