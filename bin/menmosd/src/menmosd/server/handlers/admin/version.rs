use apikit::reject::HTTPError;

use axum::Json;

use protocol::VersionResponse;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tracing::instrument(name = "handler.admin.version")]
pub async fn version() -> Result<Json<VersionResponse>, HTTPError> {
    Ok(Json(VersionResponse::new(VERSION)))
}
