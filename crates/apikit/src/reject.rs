use std::convert::Infallible;

use warp::{hyper::StatusCode, reject};

use crate::reply;

const MESSAGE_BAD_REQUEST: &str = "bad request";
const MESSAGE_FORBIDDEN: &str = "forbidden";

#[derive(Debug)]
pub struct Forbidden;

impl reject::Reject for Forbidden {}

impl From<Forbidden> for warp::Rejection {
    fn from(v: Forbidden) -> warp::Rejection {
        reject::custom(v)
    }
}

#[derive(Debug)]
pub struct BadRequest;

impl reject::Reject for BadRequest {}

impl From<BadRequest> for warp::Rejection {
    fn from(v: BadRequest) -> warp::Rejection {
        reject::custom(v)
    }
}

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

impl From<InternalServerError> for warp::Rejection {
    fn from(v: InternalServerError) -> Self {
        reject::custom(v)
    }
}

#[derive(Debug)]
pub struct NotFound;

impl reject::Reject for NotFound {}

impl From<NotFound> for warp::Rejection {
    fn from(v: NotFound) -> Self {
        reject::custom(v)
    }
}

pub async fn recover(err: warp::Rejection) -> Result<impl warp::Reply, Infallible> {
    if let Some(Forbidden) = err.find() {
        Ok(reply::error(MESSAGE_FORBIDDEN, StatusCode::FORBIDDEN))
    } else if let Some(BadRequest) = err.find() {
        Ok(reply::error(MESSAGE_BAD_REQUEST, StatusCode::BAD_REQUEST))
    } else if let Some(InternalServerError { message }) = err.find() {
        Ok(reply::error(message, StatusCode::INTERNAL_SERVER_ERROR))
    } else {
        log::warn!("unhandled rejection: {:?}", err);
        Ok(reply::error(
            "unknown error",
            StatusCode::INTERNAL_SERVER_ERROR,
        ))
    }
}
