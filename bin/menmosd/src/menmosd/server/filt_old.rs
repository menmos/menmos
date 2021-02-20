use std::{convert::Infallible, sync::Arc};

use apikit::auth::authenticated;

use interface::{message::directory_node::CertificateInfo, DirectoryNode};

use warp::Filter;

use crate::Config;

use super::handlers;

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
