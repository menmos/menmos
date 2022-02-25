use axum::extract::FromRequest;
use axum::Json;

use menmos_auth::UserIdentity;

use protocol::VersionResponse;

use apikit::reject::HTTPError;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tracing::instrument]
pub async fn version(_user: UserIdentity) -> Result<Json<VersionResponse>, HTTPError> {
    // TODO: Use FromRequest for authentication:
    // https://github.com/Z4RX/axum_jwt_example/blob/master/src/extractors.rs
    Ok(Json(VersionResponse::new(VERSION)))
}
