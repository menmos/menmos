use apikit::reject::HTTPError;

use axum::extract::Extension;
use axum::response::Response;

use interface::DynDirectoryNode;

use menmos_auth::UserIdentity;

#[tracing::instrument("handler.routing.delete", skip(node))]
pub async fn delete(
    user: UserIdentity,
    Extension(node): Extension<DynDirectoryNode>,
) -> Result<Response, HTTPError> {
    node.routing()
        .delete_routing_config(&user.username)
        .await
        .map_err(HTTPError::internal_server_error)?;

    Ok(apikit::reply::message("ok"))
}
