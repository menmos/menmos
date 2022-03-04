//! Commonly used rejections and recovery procedures.
use axum::http::StatusCode;

use axum::response::{IntoResponse, Response};

use serde::Serialize;

use crate::reply;

const MESSAGE_NOT_FOUND: &str = "not found";
const MESSAGE_FORBIDDEN: &str = "forbidden";
const MESSAGE_INTERNAL_SERVER_ERROR: &str = "internal server error";

#[derive(Serialize)]
#[serde(untagged)]
pub enum HTTPError {
    BadRequest { error: String },
    Forbidden,
    NotFound,
    InternalServerError { error: String },
}

impl HTTPError {
    pub fn bad_request<S: ToString>(s: S) -> Self {
        Self::BadRequest {
            error: s.to_string(),
        }
    }

    pub fn internal_server_error<S: ToString>(s: S) -> Self {
        Self::InternalServerError {
            error: s.to_string(),
        }
    }
}

impl IntoResponse for HTTPError {
    fn into_response(self) -> Response {
        match self {
            Self::BadRequest { error } => reply::error(error, StatusCode::BAD_REQUEST),
            Self::Forbidden => reply::error(MESSAGE_FORBIDDEN, StatusCode::FORBIDDEN),
            Self::NotFound => reply::error(MESSAGE_NOT_FOUND, StatusCode::NOT_FOUND),
            Self::InternalServerError { ref error } => {
                tracing::error!("{error}");
                reply::error(
                    MESSAGE_INTERNAL_SERVER_ERROR,
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
            }
        }
    }
}
