use apikit::auth::authenticated;

use warp::Filter;

use crate::server::{handlers, Context};

use super::util::with_context;

const BLOBS_PATH: &str = "blob";
const METADATA_PATH: &str = "metadata";

pub fn all(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    create(context.clone())
        .or(update(context.clone()))
        .or(get(context.clone()))
        .or(list(context))
}

fn create(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::put()
        .and(with_context(context))
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::param())
        .and(warp::path(METADATA_PATH))
        .and(warp::body::json())
        .and(warp::header::<String>("x-storage-id"))
        .and(warp::header::<String>("x-registration-secret"))
        .and_then(handlers::blobmeta::create)
}

fn update(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(authenticated(context.config.node.admin_password.clone()))
        .and(with_context(context))
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::param())
        .and(warp::path(METADATA_PATH))
        .and(warp::body::json())
        .and(warp::filters::addr::remote())
        .and_then(handlers::blobmeta::update)
}

fn get(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(authenticated(context.config.node.admin_password.clone()))
        .and(with_context(context))
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::param())
        .and(warp::path(METADATA_PATH))
        .and_then(handlers::blobmeta::get)
}

fn list(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(authenticated(context.config.node.admin_password.clone()))
        .and(with_context(context))
        .and(warp::path(METADATA_PATH))
        .and(warp::body::json())
        .and_then(handlers::blobmeta::list)
}
