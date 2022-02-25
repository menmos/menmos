use std::sync::Arc;

use axum::extract::Extension;
use axum::Json;

use apikit::reject::{Forbidden, HTTPError, InternalServerError};

use menmos_auth::UserIdentity;

use interface::DirectoryNode;

use protocol::directory::auth::{LoginRequest, LoginResponse};

use warp::reply;

use crate::server::Context;

#[tracing::instrument(skip(node, key), fields(user = ? request.username))]
pub async fn login(
    Extension(node): Extension<Arc<dyn DirectoryNode + Send + Sync>>,
    Extension(key): Extension<String>,
    request: Json<LoginRequest>,
) -> Result<Json<LoginResponse>, HTTPError> {
    if node
        .user()
        .login(&request.username, &request.password)
        .await
        .map_err(HTTPError::internal_server_error)?
    {
        let token = menmos_auth::make_token(
            &key,
            UserIdentity {
                username: request.username.clone(),
                admin: true, // TODO: We don't support privilege levels yet.
                blobs_whitelist: None,
            },
        )
        .map_err(HTTPError::internal_server_error)?;

        Ok(Json(LoginResponse { token }))
    } else {
        Err(HTTPError::Forbidden)
    }
}
