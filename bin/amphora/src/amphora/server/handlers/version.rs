use protocol::VersionResponse;

use warp::reply;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tracing::instrument]
pub async fn version() -> Result<reply::Response, warp::Rejection> {
    Ok(apikit::reply::json(&VersionResponse::new(VERSION)))
}
