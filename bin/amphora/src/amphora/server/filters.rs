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
const FLUSH_PATH: &str = "flush";

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
        .or(fsync(node.clone(), config.clone()))
        .or(flush(node, config))
        .or(version())
        .with(warp::log::custom(
            |info| tracing::info!(status = ?info.status(), elapsed = ?info.elapsed(), "{} {}", info.method(), info.path()),
        ))
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
        // We use X-Blob-Size because Content-Length is a liar sometimes.
        // The content length includes the size of the multipart boundary times the number of parts,
        // so it'll be higher than the real blob size.
        .and(warp::header::optional::<u64>("x-blob-size"))
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
    warp::put()
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

fn flush<N>(
    node: Arc<N>,
    config: Config,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: StorageNode + Send + Sync,
{
    warp::post()
        .and(user(config.node.encryption_key))
        .and(with_node(node))
        .and(warp::path(FLUSH_PATH))
        .and(warp::path::end())
        .and_then(handlers::flush)
}

fn version() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(warp::path(VERSION_PATH))
        .and_then(handlers::version)
}
