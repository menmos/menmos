use apikit::auth::UserIdentity;
use apikit::reject::{Forbidden, InternalServerError};

use protocol::directory::auth::{LoginResponse, RegisterRequest};

use warp::reply;

use crate::server::Context;

pub async fn register(
    identity: apikit::auth::UserIdentity,
    context: Context,
    request: RegisterRequest,
) -> Result<reply::Response, warp::Rejection> {
    tracing::debug!("identity: {:?}", identity);
    if !identity.admin {
        return Err(Forbidden.into());
    }

    // Don't allow duplicate username registration.
    if context
        .node
        .user()
        .has_user(&request.username)
        .await
        .map_err(InternalServerError::from)?
    {
        return Err(Forbidden.into());
    }

    context
        .node
        .user()
        .register(&request.username, &request.password)
        .await
        .map_err(InternalServerError::from)?;

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
}
