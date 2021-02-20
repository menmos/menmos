use apikit::reject::{Forbidden, InternalServerError};

use interface::message::MessageResponse;
use interface::BlobMeta;

use warp::reply;

use crate::server::Context;

pub async fn create(
    context: Context,
    blob_id: String,
    blob_meta: BlobMeta,
    storage_node_id: String,
    registration_secret: String,
) -> Result<reply::Response, warp::Rejection> {
    if context.config.node.registration_secret != registration_secret {
        return Err(warp::reject::custom(Forbidden));
    }

    context
        .node
        .index_blob(&blob_id, blob_meta, &storage_node_id)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&MessageResponse {
        message: "Ok".to_string(),
    }))
}
