use std::sync::Arc;

use apikit::reject::{BadRequest, InternalServerError};
use bytes::Bytes;
use headers::HeaderValue;
use interface::{message, Range, StorageNode};
use warp::reply;

pub async fn write<N: StorageNode>(
    node: Arc<N>,
    range_header: HeaderValue,
    blob_id: String,
    body: Bytes,
) -> Result<reply::Response, warp::Rejection> {
    // Fetch the request content range from the header.
    let range = Range::from_header(range_header).map_err(|_| BadRequest)?;

    node.write(blob_id, range, body)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&message::MessageResponse {
        message: "Ok".to_string(),
    }))
}
