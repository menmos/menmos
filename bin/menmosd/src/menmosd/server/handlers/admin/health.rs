use apikit::reject::HTTPError;

use crate::server::context::Context;
use axum::Json;

pub async fn health() -> Result<Json<apikit::payload::MessageResponse>, HTTPError> {
    Ok(Json(apikit::payload::MessageResponse::new("healthy")))
}
