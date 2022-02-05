use warp::Filter;

use crate::server::{handlers, Context};

use super::util::with_context;

const ROUTING_PATH: &str = "routing";

pub fn all(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    get(context.clone())
        .or(set(context.clone()))
        .or(delete(context))
}

fn get(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(menmos_auth::user(
            context.config.node.encryption_key.clone(),
        ))
        .and(with_context(context))
        .and(warp::path(ROUTING_PATH))
        .and(warp::path::end())
        .and_then(handlers::routing::get)
}

fn set(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::put()
        .and(menmos_auth::user(
            context.config.node.encryption_key.clone(),
        ))
        .and(with_context(context))
        .and(warp::path(ROUTING_PATH))
        .and(warp::path::end())
        .and(warp::body::json())
        .and_then(handlers::routing::set)
}

fn delete(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::delete()
        .and(menmos_auth::user(
            context.config.node.encryption_key.clone(),
        ))
        .and(with_context(context))
        .and(warp::path(ROUTING_PATH))
        .and(warp::path::end())
        .and_then(handlers::routing::delete)
}
