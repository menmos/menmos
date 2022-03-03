use apikit::payload::MessageResponse;
use apikit::reject::HTTPError;

use axum::Json;

#[tracing::instrument]
pub async fn health() -> Result<Json<MessageResponse>, HTTPError> {
    Ok(Json(MessageResponse::new("healthy")))
}
