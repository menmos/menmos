use std::sync::Arc;

use apikit::reject::InternalServerError;

use interface::{message::MessageResponse, StorageNode};

use warp::reply;

pub async fn fsync<N: StorageNode>(
    node: Arc<N>,
    blob_id: String,
) -> Result<reply::Response, warp::Rejection> {
    node.fsync(blob_id)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&MessageResponse {
        message: String::from("OK"),
    }))
}
