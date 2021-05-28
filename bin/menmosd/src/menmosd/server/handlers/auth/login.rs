use apikit::auth::UserIdentity;
use apikit::reject::{Forbidden, InternalServerError};

use protocol::directory::auth::{LoginRequest, LoginResponse};

use warp::reply;

use crate::server::Context;

pub async fn login(
    context: Context,
    request: LoginRequest,
) -> Result<reply::Response, warp::Rejection> {
    if context
        .node
        .user()
        .login(&request.username, &request.password)
        .await
        .map_err(InternalServerError::from)?
    {
        let token = apikit::auth::make_token(
            &context.config.node.encryption_key,
            UserIdentity {
                username: request.username,
                admin: true, // TODO: We don't support privilege levels yet.
                blobs_whitelist: None,
            },
        )
        .map_err(InternalServerError::from)?;

        Ok(apikit::reply::json(&LoginResponse { token }))
    } else {
        Err(Forbidden.into())
    }
}
