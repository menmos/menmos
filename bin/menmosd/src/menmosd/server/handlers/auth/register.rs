use apikit::reject::HTTPError;

use axum::extract::Extension;
use axum::Json;

use interface::DynDirectoryNode;

use menmos_auth::{EncryptionKey, UserIdentity};

use protocol::directory::auth::{LoginResponse, RegisterRequest};

#[tracing::instrument("handler.auth.register", skip(node, key), fields(user = ? request.username, caller = ? identity.username))]
pub async fn register(
    identity: menmos_auth::UserIdentity,
    Extension(node): Extension<DynDirectoryNode>,
    Extension(EncryptionKey { key }): Extension<EncryptionKey>,
    request: Json<RegisterRequest>,
) -> Result<Json<LoginResponse>, HTTPError> {
    if !identity.admin {
        return Err(HTTPError::Forbidden);
    }

    // Don't allow duplicate username registration.
    if node
        .user()
        .has_user(&request.username)
        .await
        .map_err(HTTPError::internal_server_error)?
    {
        return Err(HTTPError::Forbidden);
    }

    node.user()
        .register(&request.username, &request.password)
        .await
        .map_err(HTTPError::internal_server_error)?;

    let token = menmos_auth::make_token(
        &key,
        UserIdentity {
            username: request.0.username,
            admin: true, // TODO: We don't support privilege levels yet.
            blobs_whitelist: None,
        },
    )
    .map_err(HTTPError::internal_server_error)?;

    Ok(Json(LoginResponse { token }))
}
