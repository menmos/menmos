use anyhow::Result;
use apikit::reject::HTTPError;

use axum::body::Bytes;
use axum::extract::{Extension, Path, TypedHeader};
use axum::headers::Range as RangeHeader;
use axum::response::Response;

use interface::DynStorageNode;

use menmos_auth::UserIdentity;

#[tracing::instrument(skip(node, body))]
pub async fn write(
    user: UserIdentity,
    Extension(node): Extension<DynStorageNode>,
    TypedHeader(range_header): TypedHeader<RangeHeader>,
    Path(blob_id): Path<String>,
    body: Bytes,
) -> Result<Response, HTTPError> {
    // Fetch the request content range from the header.
    let mut range_it = range_header.iter();
    let range = range_it
        .next()
        .ok_or_else(|| HTTPError::bad_request("missing range"))?;

    if range_it.next().is_some() {
        return Err(HTTPError::bad_request(
            "multi-range requests are not supported, stick to a single range per request",
        ));
    }

    node.write(blob_id, range, body, &user.username)
        .await
        .map_err(HTTPError::internal_server_error)?;

    Ok(apikit::reply::message("ok"))
}
