use std::sync::Arc;

use apikit::reject::HTTPError;

use axum::extract::Extension;
use axum::Json;

use menmos_auth::UserIdentity;

use crate::Config;

#[tracing::instrument(skip(config))]
pub async fn get_config(
    user: UserIdentity,
    Extension(config): Extension<Arc<Config>>,
) -> Result<Json<Config>, HTTPError> {
    if !user.admin {
        return Err(HTTPError::Forbidden);
    }
    Ok(Json((*config).clone()))
}
