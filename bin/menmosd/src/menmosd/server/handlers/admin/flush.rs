use apikit::reject::HTTPError;

use axum::extract::Extension;
use axum::response::Response;

use interface::DynDirectoryNode;

use menmos_auth::UserIdentity;

#[tracing::instrument(name = "handler.admin.flush", skip(node))]
pub async fn flush(
    user: UserIdentity,
    Extension(node): Extension<DynDirectoryNode>,
) -> Result<Response, HTTPError> {
    if !user.admin {
        return Err(HTTPError::Forbidden);
    }

    node.flush()
        .await
        .map_err(HTTPError::internal_server_error)?;

    Ok(apikit::reply::message("ok"))
}
