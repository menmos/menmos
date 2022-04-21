use axum::Json;

use protocol::VersionResponse;

use apikit::reject::HTTPError;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tracing::instrument(name = "handler.version")]
pub async fn version() -> Result<Json<VersionResponse>, HTTPError> {
    Ok(Json(VersionResponse::new(VERSION)))
}
