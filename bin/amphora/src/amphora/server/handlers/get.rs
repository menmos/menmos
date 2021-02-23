use std::{ops::Bound, sync::Arc};
use std::{ops::Range, pin::Pin};

use anyhow::{ensure, Result};

use apikit::{
    auth::UserIdentity,
    reject::{Forbidden, InternalServerError},
};

use headers::{ContentLength, ContentRange, Header, HeaderMapExt, HeaderValue};

use http::StatusCode;

use interface::StorageNode;

use serde::{Deserialize, Serialize};

use warp::hyper::Body;
use warp::reply;

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Signature {
    pub signature: Option<String>,
}

fn add_range_info_to_response(
    range: Range<u64>,
    current_chunk_size: u64,
    total_blob_size: u64,
    response: &mut reply::Response,
) -> Result<()> {
    *response.status_mut() = StatusCode::PARTIAL_CONTENT;

    let start = range.start;
    let end = start + current_chunk_size - 1;

    let content_range = ContentRange::bytes(start..end, Some(total_blob_size))?;

    response.headers_mut().typed_insert(content_range);
    response
        .headers_mut()
        .typed_insert(ContentLength(current_chunk_size));

    Ok(())
}

fn parse_range_header(value: HeaderValue) -> Result<(Bound<u64>, Bound<u64>)> {
    // Decode the range string sent in the header value.
    let requested_ranges = headers::Range::decode(&mut vec![value].iter())?;

    // Convert the decoded range struct into a vectro of tuples of bounds.
    let ranges: Vec<(Bound<u64>, Bound<u64>)> = requested_ranges.iter().collect();
    ensure!(ranges.len() == 1, "multipart ranges not supported");

    Ok(ranges[0])
}

pub async fn get<N: StorageNode>(
    user: UserIdentity,
    node: Arc<N>,
    range_header: Option<HeaderValue>,
    blob_id: String,
) -> Result<reply::Response, warp::Rejection> {
    if let Some(whitelist) = user.blobs_whitelist {
        if !whitelist.contains(&blob_id) {
            return Err(Forbidden.into());
        }
    }

    // Fetch the request content range from the header if any.
    let range = range_header.map(|h| parse_range_header(h).ok()).flatten();

    // Get the blob stream from the backend.
    let blob = node
        .get(blob_id, range)
        .await
        .map_err(InternalServerError::from)?;

    // Check that the blob is only accessed by its owner.
    // (this doesn't break sharing because signed URLs are signed with their owner's identity)
    if blob.info.owner != user.username {
        return Err(Forbidden.into());
    }

    let stream = Pin::from(blob.stream);

    // Start building our response.
    let mut resp = reply::Response::new(Body::wrap_stream(stream));

    // Add the range info to the response if a range was used.
    if let Some(r) = range {
        add_range_info_to_response(
            repository::util::bounds_to_range(r, 0, blob.total_blob_size),
            blob.current_chunk_size,
            blob.total_blob_size,
            &mut resp,
        )
        .map_err(InternalServerError::from)?;
    }

    // If the blob has a mimetype, we want to return it as a header so browsers can use it.
    if let Some(mimetype) = blob.info.meta.metadata.get("mimetype") {
        if let Ok(hval) = HeaderValue::from_str(mimetype) {
            resp.headers_mut().append("content-type", hval);
        }
    }

    Ok(resp)
}
