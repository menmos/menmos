use std::{convert::Infallible, sync::Arc};

use interface::StorageNode;

use menmos_auth::user;

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
