use warp::Filter;

use crate::server::{handlers, Context};

use super::util::with_context;

const AUTH_PATH: &str = "auth";
const LOGIN_PATH: &str = "login";
const REGISTER_PATH: &str = "register";

pub fn all(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    login(context.clone()).or(register(context))
}

fn login(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path(AUTH_PATH))
        .and(warp::path(LOGIN_PATH))
        .and(with_context(context))
        .and(warp::body::json())
        .and_then(handlers::auth::login)
}

fn register(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path(AUTH_PATH))
        .and(warp::path(REGISTER_PATH))
        .and(apikit::auth::user(
            context.config.node.encryption_key.clone(),
        ))
        .and(with_context(context))
        .and(warp::body::json())
        .and_then(handlers::auth::register)
}
