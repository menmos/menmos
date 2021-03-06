use std::ops::Bound;
use std::sync::Arc;

use anyhow::{ensure, Result};
use apikit::{
    auth::UserIdentity,
    reject::{BadRequest, InternalServerError},
};
use bytes::Bytes;
use headers::{Header, HeaderValue};
use interface::StorageNode;
use warp::reply;

fn parse_range_header(value: HeaderValue) -> Result<(Bound<u64>, Bound<u64>)> {
    // Decode the range string sent in the header value.
    let requested_ranges = headers::Range::decode(&mut vec![value].iter())?;

    // Convert the decoded range struct into a vectro of tuples of bounds.
    let ranges: Vec<(Bound<u64>, Bound<u64>)> = requested_ranges.iter().collect();
    ensure!(ranges.len() == 1, "multipart ranges not supported");

    Ok(ranges[0])
}

pub async fn write<N: StorageNode>(
    user: UserIdentity,
    node: Arc<N>,
    range_header: HeaderValue,
    blob_id: String,
    body: Bytes,
) -> Result<reply::Response, warp::Rejection> {
    // Fetch the request content range from the header.
    let range = parse_range_header(range_header).map_err(|_| BadRequest)?;

    node.write(blob_id, range, body, &user.username)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::message("OK"))
}
