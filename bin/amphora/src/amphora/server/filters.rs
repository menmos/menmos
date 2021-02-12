use std::{convert::Infallible, sync::Arc};

use apikit::auth::authenticated;

use interface::StorageNode;

use mime::Mime;

use serde::de::DeserializeOwned;
use warp::{filters::BoxedFilter, Filter};

use super::handlers;

use crate::Config;

const HEALTH_PATH: &str = "health";
const BLOBS_PATH: &str = "blob";
const METADATA_PATH: &str = "metadata";
const VERSION_PATH: &str = "version";

fn with_node<N>(
    node: Arc<N>,
) -> impl Filter<Extract = (Arc<N>,), Error = std::convert::Infallible> + Clone
where
    N: StorageNode + Send + Sync,
{
    warp::any().map(move || node.clone())
}

fn with_config(
    cfg: Config,
) -> impl Filter<Extract = (Config,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || cfg.clone())
}

fn optq<T: 'static + Default + Send + DeserializeOwned>() -> BoxedFilter<(T,)> {
    warp::any()
        .and(warp::query().or(warp::any().map(T::default)))
        .unify()
        .boxed()
}

pub fn all<N>(
    node: Arc<N>,
    config: Config,
) -> impl Filter<Extract = impl warp::Reply, Error = Infallible> + Clone
where
    N: StorageNode + Send + Sync,
{
    health(node.clone(), config.clone())
        .or(put(node.clone(), config.clone()))
        .or(get(node.clone(), config.clone()))
        .or(write(node.clone(), config.clone()))
        .or(update_meta(config.clone(), node.clone()))
        .or(delete(node, config))
        .or(version())
        .with(warp::log("storage::api"))
        .recover(apikit::reject::recover)
}

fn health<N>(
    node: Arc<N>,
    config: Config,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: StorageNode + Send + Sync,
{
    warp::get()
        .and(authenticated(config.node.admin_password))
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
        .and(authenticated(config.node.admin_password))
        .and(with_node(node))
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::param())
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
        .and(authenticated(config.node.admin_password))
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
        .and(authenticated(config.node.admin_password))
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
        .and(with_config(config))
        .and(with_node(node))
        .and(warp::header::optional::<String>("authorization"))
        .and(warp::header::optional("range"))
        .and(optq::<handlers::Signature>())
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
        .and(authenticated(config.node.admin_password))
        .and(with_node(node))
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::param())
        .and_then(handlers::delete)
}

fn version() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(warp::path(VERSION_PATH))
        .and_then(handlers::version)
}
