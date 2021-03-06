//! Commonly used rejections and recovery procedures.
use std::convert::Infallible;

use warp::{hyper::StatusCode, reject};

use crate::reply;

const MESSAGE_BAD_REQUEST: &str = "bad request";
const MESSAGE_FORBIDDEN: &str = "forbidden";

/// Maps to an HTTP 403.
#[derive(Debug)]
pub struct Forbidden;

impl reject::Reject for Forbidden {}

/// Maps to an HTTP 400.
#[derive(Debug)]
pub struct BadRequest;

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
    if let Some(Forbidden) = err.find() {
        Ok(reply::error(MESSAGE_FORBIDDEN, StatusCode::FORBIDDEN))
    } else if let Some(BadRequest) = err.find() {
        Ok(reply::error(MESSAGE_BAD_REQUEST, StatusCode::BAD_REQUEST))
    } else if let Some(InternalServerError { message }) = err.find() {
        Ok(reply::error(message, StatusCode::INTERNAL_SERVER_ERROR))
    } else if let Some(_e) = err.find::<warp::filters::body::BodyDeserializeError>() {
        Ok(reply::error(MESSAGE_BAD_REQUEST, StatusCode::BAD_REQUEST))
    } else {
        log::warn!("unhandled rejection: {:?}", err);
        Ok(reply::error(
            "unknown error",
            StatusCode::INTERNAL_SERVER_ERROR,
        ))
    }
}
