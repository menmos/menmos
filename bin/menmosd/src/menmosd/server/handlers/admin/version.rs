use apikit::reject::HTTPError;

use axum::Json;

use menmos_auth::UserIdentity;

use protocol::VersionResponse;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tracing::instrument]
pub async fn version(_user: UserIdentity) -> Result<Json<VersionResponse>, HTTPError> {
    Ok(Json(VersionResponse::new(VERSION)))
}
