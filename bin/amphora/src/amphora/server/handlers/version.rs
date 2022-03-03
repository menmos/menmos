use axum::Json;

use protocol::VersionResponse;

use apikit::reject::HTTPError;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tracing::instrument]
pub async fn version() -> Result<Json<VersionResponse>, HTTPError> {
    Ok(Json(VersionResponse::new(VERSION)))
}
