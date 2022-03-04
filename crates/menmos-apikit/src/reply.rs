//! Shorthand reply utilities.
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;

use crate::payload;

/// Return a JSON error reply with a custom status code.
pub fn error<T: ToString>(e: T, code: StatusCode) -> Response {
    (
        code,
        Json(payload::ErrorResponse {
            error: e.to_string(),
        }),
    )
        .into_response()
}

/// Return a JSON message.
pub fn message<M: Into<String>>(message: M) -> Response {
    Json(&payload::MessageResponse::new(message)).into_response()
}
