use apikit::reject::HTTPError;

use axum::response::Response;

pub async fn health() -> Result<Response, HTTPError> {
    Ok(apikit::reply::message("healthy"))
}
