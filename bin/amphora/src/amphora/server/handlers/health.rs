use apikit::reject::HTTPError;

use axum::response::Response;

#[tracing::instrument]
pub async fn health() -> Result<Response, HTTPError> {
    Ok(apikit::reply::message("healthy"))
}
