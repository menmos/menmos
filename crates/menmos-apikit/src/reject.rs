//! Commonly used rejections and recovery procedures.
use std::convert::Infallible;

use axum::body::{Body, Bytes, Full};
use axum::response::{IntoResponse, Response};
use axum::Json;

use serde::Serialize;

use warp::{hyper::StatusCode, reject};

use crate::reply;

const MESSAGE_BAD_REQUEST: &str = "bad request";
const MESSAGE_FORBIDDEN: &str = "forbidden";

#[derive(Serialize)]
#[serde(untagged)]
pub enum HTTPError {
    BadRequest { error: String },
    Forbidden,
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
        let status = match self {
            Self::BadRequest { .. } => StatusCode::BAD_REQUEST,
            Self::Forbidden => StatusCode::FORBIDDEN, // TODO: Actually put a message here.
            Self::InternalServerError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let body = Json(self);

        (status, body).into_response()
    }
}

/// Maps to an HTTP 403.
#[derive(Debug)]
pub struct Forbidden;

impl reject::Reject for Forbidden {}

/// Maps to an HTTP 400.
#[derive(Debug)]
pub struct BadRequest {
    message: String,
}

impl<E: ToString> From<E> for BadRequest {
    fn from(e: E) -> Self {
        BadRequest {
            message: e.to_string(),
        }
    }
}

impl reject::Reject for BadRequest {}

/// Maps to an HTTP 500.
#[derive(Debug)]
pub struct InternalServerError {
    message: String,
}

impl<E: ToString> From<E> for InternalServerError {
    fn from(e: E) -> Self {
        InternalServerError {
            message: e.to_string(),
        }
    }
}

impl reject::Reject for InternalServerError {}

/// Maps to an HTTP 404.
#[derive(Debug)]
pub struct NotFound;

impl reject::Reject for NotFound {}

/// Catch-all recover routine.
///
/// Catches all rejections defined in this module, converting them to the proper HTTP status code.
/// Converts all remaining unknown rejections to an HTTP 500.
pub async fn recover(err: warp::Rejection) -> Result<impl warp::Reply, Infallible> {
    if let Some(Forbidden {}) = err.find() {
        tracing::info!("rejection: Forbidden");
        Ok(reply::error(MESSAGE_FORBIDDEN, StatusCode::FORBIDDEN))
    } else if let Some(BadRequest { message }) = err.find() {
        tracing::info!("rejection: BadRequest");
        Ok(reply::error(
            format!("{}: {}", MESSAGE_BAD_REQUEST, message),
            StatusCode::BAD_REQUEST,
        ))
    } else if let Some(InternalServerError { message }) = err.find() {
        tracing::error!("error: {}", message);
        Ok(reply::error(message, StatusCode::INTERNAL_SERVER_ERROR))
    } else if let Some(e) = err.find::<warp::filters::body::BodyDeserializeError>() {
        tracing::info!(info = ?e.to_string(), "rejection: BadRequest");
        Ok(reply::error(
            format!("{}: {}", MESSAGE_BAD_REQUEST, &e.to_string()),
            StatusCode::BAD_REQUEST,
        ))
    } else {
        tracing::error!("unhandled rejection: {:?}", err);
        Ok(reply::error(
            "unknown error",
            StatusCode::INTERNAL_SERVER_ERROR,
        ))
    }
}
