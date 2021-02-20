use apikit::auth::UserIdentity;
use interface::message::VersionResponse;

use warp::reply;

const VERSION: &str = env!("CARGO_PKG_VERSION");

pub async fn version(_user: UserIdentity) -> Result<reply::Response, warp::Rejection> {
    Ok(apikit::reply::json(&VersionResponse {
        version: VERSION.to_string(),
    }))
}
