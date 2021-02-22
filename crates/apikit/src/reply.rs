//! Shorthand reply utilities.
use serde::Serialize;

use warp::reply;
use warp::{http::StatusCode, Reply};

use crate::payload;

/// Return an JSON error reply with a custom status code.
pub fn error<T: ToString>(e: T, code: StatusCode) -> reply::Response {
    reply::with_status(
        reply::json(&payload::ErrorResponse {
            error: e.to_string(),
        }),
        code,
    )
    .into_response()
}

/// Return a JSON message.
pub fn message<M: Into<String>>(message: M) -> reply::Response {
    return json(&payload::MessageResponse::new(message));
}

/// Thin wrapper around `warp::reply::json()` that casts the return value into a `warp::reply::Response` struct.
pub fn json<T: Serialize>(payload: &T) -> reply::Response {
    reply::json(payload).into_response()
}
