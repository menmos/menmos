use std::{convert::Infallible, sync::Arc};

use apikit::auth::authenticated;

use interface::{message::directory_node::CertificateInfo, DirectoryNode};

use warp::Filter;

use crate::Config;

use super::handlers;

const HEALTH_PATH: &str = "health";
const NODES_PATH: &str = "node";
const STORAGE_PATH: &str = "storage";
const BLOBS_PATH: &str = "blob";
const QUERY_PATH: &str = "query";
const METADATA_PATH: &str = "metadata";
const COMMIT_PATH: &str = "commit";
const VERSION_PATH: &str = "version";
const REBUILD_PATH: &str = "rebuild";
const FSYNC_PATH: &str = "fsync";

fn with_node<N>(
    node: Arc<N>,
) -> impl Filter<Extract = (Arc<N>,), Error = std::convert::Infallible> + Clone
where
    N: DirectoryNode + Send + Sync,
{
    warp::any().map(move || node.clone())
}

fn with_config(
    cfg: Config,
) -> impl Filter<Extract = (Config,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || cfg.clone())
}

fn with_certificates(
    certs: Option<CertificateInfo>,
) -> impl Filter<Extract = (Option<CertificateInfo>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || certs.clone())
}

pub fn all<N>(
    node: Arc<N>,
    config: Config,
    certificate_info: Option<CertificateInfo>,
) -> impl Filter<Extract = impl warp::Reply, Error = Infallible> + Clone
where
    N: DirectoryNode + Send + Sync,
{
    health(node.clone())
        .or(query(config.clone(), node.clone()))
        .or(register_storage_node(
            config.clone(),
            node.clone(),
            certificate_info,
        ))
        .or(list_storage_nodes(config.clone(), node.clone()))
        .or(update(config.clone(), node.clone()))
        .or(update_meta(config.clone(), node.clone()))
        .or(write(config.clone(), node.clone()))
        .or(get(config.clone(), node.clone()))
        .or(get_meta(config.clone(), node.clone()))
        .or(delete(config.clone(), node.clone()))
        .or(put(config.clone(), node.clone()))
        .or(index_blob(config.clone(), node.clone()))
        .or(list_metadata(config.clone(), node.clone()))
        .or(rebuild(config.clone(), node.clone()))
        .or(rebuild_complete(config.clone(), node.clone()))
        .or(commit(config.clone(), node.clone()))
        .or(fsync(config.clone(), node))
        .or(version(config))
        .recover(apikit::reject::recover)
        .with(warp::log("directory::api"))
}

fn health<N>(
    node: Arc<N>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: DirectoryNode + Send + Sync,
{
    warp::get()
        .and(warp::path(HEALTH_PATH))
        .and(with_node(node))
        .and_then(handlers::health)
}

fn register_storage_node<N>(
    config: Config,
    node: Arc<N>,
    certs: Option<CertificateInfo>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: DirectoryNode + Send + Sync,
{
    warp::put()
        .and(warp::path(NODES_PATH))
        .and(warp::path(STORAGE_PATH))
        .and(with_node(node))
        .and(with_config(config))
        .and(with_certificates(certs))
        .and(warp::header::<String>("x-registration-secret"))
        .and(warp::body::json())
        .and_then(handlers::register_storage_node)
}

fn list_storage_nodes<N>(
    config: Config,
    node: Arc<N>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: DirectoryNode + Send + Sync,
{
    warp::get()
        .and(authenticated(config.node.admin_password))
        .and(warp::path(NODES_PATH))
        .and(warp::path(STORAGE_PATH))
        .and(with_node(node))
        .and_then(handlers::list_storage_nodes)
}

fn put<N>(
    config: Config,
    node: Arc<N>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: DirectoryNode + Send + Sync,
{
    warp::post()
        .and(authenticated(config.node.admin_password.clone()))
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::end())
        .and(with_config(config))
        .and(with_node(node))
        .and(warp::header::<String>("x-blob-meta"))
        .and(warp::filters::addr::remote())
        .and_then(handlers::put)
}

fn index_blob<N>(
    config: Config,
    node: Arc<N>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: DirectoryNode + Send + Sync,
{
    warp::put()
        .and(with_config(config))
        .and(with_node(node))
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::param())
        .and(warp::path(METADATA_PATH))
        .and(warp::body::json())
        .and(warp::header::<String>("x-storage-id"))
        .and(warp::header::<String>("x-registration-secret"))
        .and_then(handlers::index_blob)
}

fn update_meta<N>(
    config: Config,
    node: Arc<N>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: DirectoryNode + Send + Sync,
{
    warp::post()
        .and(authenticated(config.node.admin_password.clone()))
        .and(with_config(config))
        .and(with_node(node))
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::param())
        .and(warp::path(METADATA_PATH))
        .and(warp::body::json())
        .and(warp::filters::addr::remote())
        .and_then(handlers::update_meta)
}

// Full overwrite of a blob.
fn update<N>(
    config: Config,
    node: Arc<N>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: DirectoryNode + Send + Sync,
{
    warp::post()
        .and(authenticated(config.node.admin_password.clone()))
        .and(with_config(config))
        .and(with_node(node))
        .and(warp::header::<String>("x-blob-meta"))
        .and(warp::filters::addr::remote())
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::param())
        .and(warp::path::end())
        .and(warp::body::stream())
        .and_then(handlers::update)
}

// Random write to a blob.
fn write<N>(
    config: Config,
    node: Arc<N>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: DirectoryNode + Send + Sync,
{
    warp::put()
        .and(authenticated(config.node.admin_password.clone()))
        .and(with_config(config))
        .and(with_node(node))
        .and(warp::filters::addr::remote())
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::param())
        .and(warp::path::end())
        .and(warp::body::stream())
        .and_then(handlers::write)
}

fn get<N>(
    config: Config,
    node: Arc<N>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: DirectoryNode + Send + Sync,
{
    warp::get()
        .and(authenticated(config.node.admin_password.clone()))
        .and(with_config(config))
        .and(with_node(node))
        .and(warp::filters::addr::remote())
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::param())
        .and(warp::path::end())
        .and_then(handlers::get)
}

fn get_meta<N>(
    config: Config,
    node: Arc<N>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: DirectoryNode + Send + Sync,
{
    warp::get()
        .and(authenticated(config.node.admin_password))
        .and(with_node(node))
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::param())
        .and(warp::path(METADATA_PATH))
        .and_then(handlers::get_meta)
}

fn delete<N>(
    config: Config,
    node: Arc<N>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: DirectoryNode + Send + Sync,
{
    warp::delete()
        .and(authenticated(config.node.admin_password.clone()))
        .and(with_config(config))
        .and(with_node(node))
        .and(warp::filters::addr::remote())
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::param())
        .and_then(handlers::delete)
}

fn query<N>(
    config: Config,
    node: Arc<N>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: DirectoryNode + Send + Sync,
{
    warp::post()
        .and(authenticated(config.node.admin_password.clone()))
        .and(with_config(config))
        .and(with_node(node))
        .and(warp::path(QUERY_PATH))
        .and(warp::filters::addr::remote())
        .and(warp::body::json())
        .and_then(handlers::query)
}

fn list_metadata<N>(
    config: Config,
    node: Arc<N>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: DirectoryNode + Send + Sync,
{
    warp::get()
        .and(authenticated(config.node.admin_password))
        .and(with_node(node))
        .and(warp::path(METADATA_PATH))
        .and(warp::body::json())
        .and_then(handlers::list_metadata)
}

fn commit<N>(
    config: Config,
    node: Arc<N>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: DirectoryNode + Send + Sync,
{
    warp::get()
        .and(authenticated(config.node.admin_password))
        .and(with_node(node))
        .and(warp::path(COMMIT_PATH))
        .and_then(handlers::commit)
}

fn rebuild<N>(
    config: Config,
    node: Arc<N>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: DirectoryNode + Send + Sync,
{
    warp::post()
        .and(authenticated(config.node.admin_password))
        .and(with_node(node))
        .and(warp::path(REBUILD_PATH))
        .and_then(handlers::rebuild)
}

fn rebuild_complete<N>(
    config: Config,
    node: Arc<N>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: DirectoryNode + Send + Sync,
{
    warp::delete()
        .and(authenticated(config.node.admin_password))
        .and(with_node(node))
        .and(warp::path(REBUILD_PATH))
        .and(warp::path::param())
        .and_then(handlers::rebuild_complete)
}

fn fsync<N>(
    config: Config,
    node: Arc<N>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
where
    N: DirectoryNode + Send + Sync,
{
    warp::post()
        .and(authenticated(config.node.admin_password.clone()))
        .and(with_config(config))
        .and(with_node(node))
        .and(warp::path(BLOBS_PATH))
        .and(warp::path::param())
        .and(warp::path(FSYNC_PATH))
        .and(warp::path::end())
        .and(warp::filters::addr::remote())
        .and_then(handlers::fsync)
}

fn version(
    config: Config,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(authenticated(config.node.admin_password))
        .and(warp::path(VERSION_PATH))
        .and_then(handlers::version)
}
