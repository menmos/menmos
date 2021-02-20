use serde::Serialize;

use warp::reply;
use warp::{http::StatusCode, Reply};

use crate::payload;

pub fn error<T: ToString>(e: T, code: StatusCode) -> reply::Response {
    reply::with_status(
        reply::json(&payload::ErrorResponse {
            error: e.to_string(),
        }),
        code,
    )
    .into_response()
}

pub fn message<M: Into<String>>(message: M) -> reply::Response {
    return json(&payload::MessageResponse::new(message));
}

pub fn json<T: Serialize>(payload: &T) -> reply::Response {
    reply::json(payload).into_response()
}
