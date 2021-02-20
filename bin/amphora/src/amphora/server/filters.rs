use std::{convert::Infallible, sync::Arc};

use apikit::auth::user;

use interface::StorageNode;

use mime::Mime;

use warp::Filter;

use super::handlers;

use crate::Config;

const HEALTH_PATH: &str = "health";
const BLOBS_PATH: &str = "blob";
const METADATA_PATH: &str = "metadata";
const VERSION_PATH: &str = "version";
const FSYNC_PATH: &str = "fsync";

fn with_node<N>(
    node: Arc<N>,
) -> impl Filter<Extract = (Arc<N>,), Error = std::convert::Infallible> + Clone
where
    N: StorageNode + Send + Sync,
{
    warp::any().map(move || node.clone())
}

pub fn all<N>(
    node: Arc<N>,
    config: Config,
) -> impl Filter<Extract = impl warp::Reply, Error = Infallible> + Clone
where
    N: StorageNode + Send + Sync,
{
    health(node.clone())
        .or(put(node.clone(), config.clone()))
        .or(get(node.clone(), config.clone()))
        .or(write(node.clone(), config.clone()))
        .or(update_meta(config.clone(), node.clone()))
        .or(delete(node.clone(), config.clone()))
        .or(fsync(node, config))
        .or(version())
        .with(warp::log("storage::api"))
        .recover(apikit::reject::recover)
}

fn health<N>(
    node: Arc<N>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: StorageNode + Send + Sync,
{
    warp::get()
        .and(warp::path(HEALTH_PATH))
        .and(with_node(node))
        .and_then(handlers::health)
}

fn put<N>(
    node: Arc<N>,
    config: Config,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: StorageNode + Send + Sync,
{
    warp::post()
        .and(user(config.node.encryption_key))
        .and(with_node(node))
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::param())
        .and(warp::path::end())
        .and(warp::header::optional::<Mime>("content-type"))
        .and(warp::header::value("x-blob-meta"))
        .and(warp::body::stream())
        .and_then(handlers::put)
}

fn update_meta<N>(
    config: Config,
    node: Arc<N>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: StorageNode + Send + Sync,
{
    warp::post()
        .and(user(config.node.encryption_key))
        .and(with_node(node))
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::param())
        .and(warp::path(METADATA_PATH))
        .and(warp::body::json())
        .and_then(handlers::update_meta)
}

fn write<N>(
    node: Arc<N>,
    config: Config,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: StorageNode + Send + Sync,
{
    warp::put()
        .and(user(config.node.encryption_key))
        .and(with_node(node))
        .and(warp::header("range"))
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::param())
        .and(warp::body::bytes())
        .and_then(handlers::write)
}

fn get<N>(
    node: Arc<N>,
    config: Config,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: StorageNode + Send + Sync,
{
    warp::get()
        .and(user(config.node.encryption_key))
        .and(with_node(node))
        .and(warp::header::optional("range"))
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::param())
        .and_then(handlers::get)
}

fn delete<N>(
    node: Arc<N>,
    config: Config,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: StorageNode + Send + Sync,
{
    warp::delete()
        .and(user(config.node.encryption_key))
        .and(with_node(node))
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::param())
        .and_then(handlers::delete)
}

fn fsync<N>(
    node: Arc<N>,
    config: Config,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: StorageNode + Send + Sync,
{
    warp::post()
        .and(user(config.node.encryption_key))
        .and(with_node(node))
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::param())
        .and(warp::path(FSYNC_PATH))
        .and(warp::path::end())
        .and_then(handlers::fsync)
}

fn version() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(warp::path(VERSION_PATH))
        .and_then(handlers::version)
}
