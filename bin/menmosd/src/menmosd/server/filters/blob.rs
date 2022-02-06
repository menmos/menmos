//! Blob-related routes.
//! /blob/*

use menmos_auth::user;
use warp::Filter;

use crate::server::{handlers, Context};

use super::util::with_context;

const BLOBS_PATH: &str = "blob";
const FSYNC_PATH: &str = "fsync";

pub fn all(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    put(context.clone())
        .or(update(context.clone()))
        .or(write(context.clone()))
        .or(get(context.clone()))
        .or(delete(context.clone()))
        .or(fsync(context))
}

// Create blob.
fn put(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(user(context.config.node.encryption_key.clone()))
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::end())
        .and(with_context(context))
        .and(warp::header::<String>("x-blob-meta"))
        .and(warp::filters::addr::remote())
        .and_then(handlers::blob::put)
}

// Full overwrite of a blob.
fn update(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(user(context.config.node.encryption_key.clone()))
        .and(with_context(context))
        .and(warp::filters::addr::remote())
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::param())
        .and(warp::path::end())
        .and_then(handlers::blob::update)
}

// Random write to a blob.
fn write(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::put()
        .and(user(context.config.node.encryption_key.clone()))
        .and(with_context(context))
        .and(warp::filters::addr::remote())
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::param())
        .and(warp::path::end())
        .and(warp::body::stream())
        .and_then(handlers::blob::write)
}

// Get blob (full or range)
fn get(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(user(context.config.node.encryption_key.clone()))
        .and(with_context(context))
        .and(warp::filters::addr::remote())
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::param())
        .and(warp::path::end())
        .and_then(handlers::blob::get)
}

// Delete blob.
fn delete(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::delete()
        .and(user(context.config.node.encryption_key.clone()))
        .and(with_context(context))
        .and(warp::filters::addr::remote())
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::param())
        .and_then(handlers::blob::delete)
}

// Fsync a blob, if possible.
fn fsync(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(user(context.config.node.encryption_key.clone()))
        .and(with_context(context))
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::param())
        .and(warp::path(FSYNC_PATH))
        .and(warp::path::end())
        .and(warp::filters::addr::remote())
        .and_then(handlers::blob::fsync)
}
