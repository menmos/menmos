use apikit::auth::authenticated;

use warp::Filter;

use crate::server::{handlers, Context};

use super::util::with_context;

const COMMIT_PATH: &str = "commit";
const VERSION_PATH: &str = "version";
const REBUILD_PATH: &str = "rebuild";
const HEALTH_PATH: &str = "health";

pub fn all(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    health()
        .or(commit(context.clone()))
        .or(rebuild(context.clone()))
        .or(rebuild_complete(context.clone()))
        .or(version(context))
}

fn health() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(warp::path(HEALTH_PATH))
        .and_then(handlers::admin::health)
}

fn commit(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(authenticated(context.config.node.admin_password.clone()))
        .and(with_context(context))
        .and(warp::path(COMMIT_PATH))
        .and_then(handlers::admin::commit)
}

fn rebuild(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(authenticated(context.config.node.admin_password.clone()))
        .and(with_context(context))
        .and(warp::path(REBUILD_PATH))
        .and_then(handlers::admin::rebuild)
}

fn rebuild_complete(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::delete()
        .and(authenticated(context.config.node.admin_password.clone()))
        .and(with_context(context))
        .and(warp::path(REBUILD_PATH))
        .and(warp::path::param())
        .and_then(handlers::admin::rebuild_complete)
}

fn version(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(authenticated(context.config.node.admin_password.clone()))
        .and(warp::path(VERSION_PATH))
        .and_then(handlers::admin::version)
}
