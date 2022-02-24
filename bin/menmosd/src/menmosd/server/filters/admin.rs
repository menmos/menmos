use menmos_auth::user;

use warp::Filter;

use crate::server::{handlers, Context};

use super::util::with_context;

const VERSION_PATH: &str = "version";
const REBUILD_PATH: &str = "rebuild";
const HEALTH_PATH: &str = "health";
const FLUSH_PATH: &str = "flush";
const CONFIG_PATH: &str = "config";

pub fn all(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    rebuild(context.clone())
        .or(rebuild_complete(context.clone()))
        .or(flush(context.clone()))
        .or(version(context.clone()))
        .or(get_config(context))
}

fn rebuild(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(user(context.config.node.encryption_key.clone()))
        .and(with_context(context))
        .and(warp::path(REBUILD_PATH))
        .and_then(handlers::admin::rebuild)
}

fn rebuild_complete(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::delete()
        .and(menmos_auth::storage_node(
            context.config.node.encryption_key.clone(),
        ))
        .and(with_context(context))
        .and(warp::path(REBUILD_PATH))
        .and(warp::path::param())
        .and_then(handlers::admin::rebuild_complete)
}

fn flush(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(user(context.config.node.encryption_key.clone()))
        .and(with_context(context))
        .and(warp::path(FLUSH_PATH))
        .and(warp::path::end())
        .and_then(handlers::admin::flush)
}

fn version(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(user(context.config.node.encryption_key.clone()))
        .and(warp::path(VERSION_PATH))
        .and_then(handlers::admin::version)
}

fn get_config(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(user(context.config.node.encryption_key.clone()))
        .and(with_context(context))
        .and(warp::path(CONFIG_PATH))
        .and(warp::path::end())
        .and_then(handlers::admin::get_config)
}
