//! Storage node routes.
//! /node/storage/*
use apikit::auth::user;

use warp::Filter;

use crate::server::{handlers, Context};

use super::util;

const NODES_PATH: &str = "node";
const STORAGE_PATH: &str = "storage";

pub fn all(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    put(context.clone()).or(list(context))
}

fn put(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::put()
        .and(warp::path(NODES_PATH))
        .and(warp::path(STORAGE_PATH))
        .and(apikit::auth::storage_node(
            context.config.node.encryption_key.clone(),
        ))
        .and(util::with_context(context))
        .and(warp::body::json())
        .and(warp::filters::addr::remote())
        .and_then(handlers::storage::put)
}

fn list(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(user(context.config.node.encryption_key.clone()))
        .and(util::with_context(context))
        .and(warp::path(NODES_PATH))
        .and(warp::path(STORAGE_PATH))
        .and_then(handlers::storage::list)
}
