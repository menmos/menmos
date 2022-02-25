use axum::extract::FromRequest;
use axum::Json;

use menmos_auth::UserIdentity;

use protocol::VersionResponse;

use apikit::reject::HTTPError;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tracing::instrument]
pub async fn version(_user: UserIdentity) -> Result<Json<VersionResponse>, HTTPError> {
    Ok(Json(VersionResponse::new(VERSION)))
}
