use menmos_auth::UserIdentity;

use protocol::VersionResponse;

use warp::reply;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tracing::instrument]
pub async fn version(_user: UserIdentity) -> Result<reply::Response, warp::Rejection> {
    // TODO: Use FromRequest for authentication:
    // https://github.com/Z4RX/axum_jwt_example/blob/master/src/extractors.rs
    Ok(apikit::reply::json(&VersionResponse::new(VERSION)))
}
