use std::sync::Arc;

use apikit::reject::InternalServerError;

use interface::{message, BlobMeta, StorageNode};

pub async fn update_meta<N: StorageNode>(
    node: Arc<N>,
    blob_id: String,
    meta: BlobMeta,
) -> Result<warp::reply::Response, warp::Rejection> {
    node.update_meta(blob_id, meta)
        .await
        .map_err(InternalServerError::from)?;
    Ok(apikit::reply::json(&message::MessageResponse {
        message: String::from("OK"),
    }))
}
