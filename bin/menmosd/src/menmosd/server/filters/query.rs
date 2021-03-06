use apikit::auth::user;

use warp::Filter;

use crate::server::{handlers, Context};

use super::util::with_context;

const QUERY_PATH: &str = "query";

pub fn all(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    query(context)
}

fn query(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(user(context.config.node.encryption_key.clone()))
        .and(with_context(context))
        .and(warp::path(QUERY_PATH))
        .and(warp::filters::addr::remote())
        .and(warp::body::json())
        .and_then(handlers::query::query)
}
