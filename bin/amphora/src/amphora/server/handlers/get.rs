use std::ops::Range;
use std::pin::Pin;

use anyhow::Result;
use axum::body::StreamBody;
use axum::extract::{Extension, Path, TypedHeader};
use axum::headers::Range as RangeHeader;
use axum::response::{IntoResponse, Response};

use apikit::reject::HTTPError;

use headers::{ContentLength, ContentRange, HeaderMapExt, HeaderValue};
use http::response::Builder;

use http::StatusCode;

use interface::{DynStorageNode, FieldValue};

use menmos_auth::UserIdentity;

use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Signature {
    pub signature: Option<String>,
}

fn add_range_info_to_response(
    range: Range<u64>,
    current_chunk_size: u64,
    total_blob_size: u64,
    mut response: Builder,
) -> Result<Builder> {
    response = response.status(StatusCode::PARTIAL_CONTENT);

    let start = range.start;
    let end = start + current_chunk_size - 1;

    if let Some(headers_mut) = response.headers_mut() {
        let content_range = ContentRange::bytes(start..end, Some(total_blob_size))?;
        headers_mut.typed_insert(content_range);
        headers_mut.typed_insert(ContentLength(current_chunk_size))
    } else {
        // Builder is errored out and will fail elsewhere (we're not swallowing errors here),
        // this log is only to help debug.
        tracing::warn!("skipped header insertion because of errored-out builder");
    }

    Ok(response)
}

#[tracing::instrument(level = "info", skip(node, range_header))]
pub async fn get(
    user: UserIdentity,
    Extension(node): Extension<DynStorageNode>,
    range_header: Option<TypedHeader<RangeHeader>>,
    Path(blob_id): Path<String>,
) -> Result<impl IntoResponse, HTTPError> {
    if let Some(whitelist) = user.blobs_whitelist {
        if !whitelist.contains(&blob_id) {
            return Err(HTTPError::Forbidden);
        }
    }

    // Fetch the request content range from the header if any.
    let range = if let Some(TypedHeader(http_range)) = range_header {
        let mut range_it = http_range.iter();
        let range = range_it.next();
        if range_it.next().is_some() {
            return Err(HTTPError::bad_request(
                "multi-range requests are not supported, stick to a single range per request",
            ));
        }
        range
    } else {
        None
    };

    // Get the blob stream from the backend.
    let blob = node
        .get(blob_id, range)
        .await
        .map_err(HTTPError::internal_server_error)?;

    // Check that the blob is only accessed by its owner.
    // (this doesn't break sharing because signed URLs are signed with their owner's identity)
    if blob.info.owner != user.username {
        return Err(HTTPError::Forbidden);
    }

    let stream = Pin::from(blob.stream);

    // Start building our response.
    let mut resp = Response::builder();

    // Add the range info to the response if a range was used.
    if let Some(r) = range {
        resp = add_range_info_to_response(
            repository::util::bounds_to_range(r, 0, blob.total_blob_size),
            blob.current_chunk_size,
            blob.total_blob_size,
            resp,
        )
        .map_err(|_| HTTPError::Forbidden)?; // TODO: Review error code?
    }

    // If the blob has a string mimetype, we want to return it as a header so browsers can use it.
    if let Some(FieldValue::Str(mimetype)) = blob.info.meta.fields.get("content-type") {
        if let Ok(hval) = HeaderValue::from_str(mimetype) {
            resp = resp.header("content-type", hval);
        }
    }

    Ok(resp
        .body(StreamBody::new(stream))
        .map_err(HTTPError::internal_server_error))
}
