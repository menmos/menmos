use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;

use apikit::reject::{Forbidden, InternalServerError};

use headers::{ContentLength, ContentRange, HeaderMapExt, HeaderValue};

use http::StatusCode;

use interface::{Range, StorageNode};

use serde::{Deserialize, Serialize};

use warp::hyper::Body;
use warp::reply;

use crate::Config;

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Signature {
    pub signature: Option<String>,
}

fn validate_signed_url(
    signature: Signature,
    for_blob_id: &str,
    encryption_key: &str,
) -> Result<(), warp::Rejection> {
    if let Some(token) = signature.signature {
        urlsign::validate(&token, for_blob_id, encryption_key).map_err(|_| Forbidden)?;
        Ok(())
    } else {
        Err(warp::reject::custom(Forbidden))
    }
}

fn add_range_info_to_response(
    range: Range,
    current_chunk_size: u64,
    total_blob_size: u64,
    response: &mut reply::Response,
) -> Result<()> {
    *response.status_mut() = StatusCode::PARTIAL_CONTENT;
    let content_range = ContentRange::bytes(
        range.get_offset_range(current_chunk_size),
        Some(total_blob_size),
    )?;

    response.headers_mut().typed_insert(content_range);
    response
        .headers_mut()
        .typed_insert(ContentLength(current_chunk_size));

    Ok(())
}

pub async fn get<N: StorageNode>(
    config: Config,
    node: Arc<N>,
    user_password: Option<String>,
    range_header: Option<HeaderValue>,
    signature: Signature,
    blob_id: String,
) -> Result<reply::Response, warp::Rejection> {
    // Validate authentication _or_ pre-signed URL.
    apikit::auth::validate_password(user_password, &config.node.admin_password)
        .await
        .or_else(|_| validate_signed_url(signature, &blob_id, &config.node.encryption_key))?;

    // Fetch the request content range from the header if any.
    let range = range_header.map(|h| Range::from_header(h).ok()).flatten();

    // Get the blob stream from the backend.
    let blob = node
        .get(blob_id, range.clone())
        .await
        .map_err(InternalServerError::from)?;
    let stream = Pin::from(blob.stream);

    // Start building our response.
    let mut resp = reply::Response::new(Body::wrap_stream(stream));

    // Add the range info to the response if a range was used.
    if let Some(r) = range {
        add_range_info_to_response(r, blob.current_chunk_size, blob.total_blob_size, &mut resp)
            .map_err(InternalServerError::from)?;
    }

    // If the blob has a mimetype, we want to return it as a header so browsers can use it.
    if let Some(mimetype) = blob.meta.metadata.get("mimetype") {
        if let Ok(hval) = HeaderValue::from_str(mimetype) {
            resp.headers_mut().append("content-type", hval);
        }
    }

    Ok(resp)
}
