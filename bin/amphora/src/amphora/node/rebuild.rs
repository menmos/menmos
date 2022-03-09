use std::sync::Arc;

use anyhow::Result;
use futures::StreamExt;

use super::{directory_proxy::DirectoryProxy, index::Index};

pub struct Params {
    pub storage_node_name: String,
    pub directory_host_url: String,
    pub directory_host_port: usize,
}

pub async fn execute(parameters: Params, proxy: Arc<DirectoryProxy>, db: Arc<Index>) -> Result<()> {
    tracing::info!("starting node rebuild");

    // Step 1 - Get a set of keys to push (so we dont re-push documents that are indexed during the rebuild).
    let keys = db.get_all_keys()?;

    // Step 2 - Push all those keys (and their meta) back to the directory.
    let puts = futures::stream::iter(keys.into_iter().map(|key| {
        let cloned_proxy = proxy.clone();
        let cloned_db = db.clone();
        let cloned_node_id = parameters.storage_node_name.clone();
        async move {
            let info_maybe = cloned_db.get(&key)?;
            if info_maybe.is_none() {
                tracing::warn!(
                    blob_id= ?key,
                    "seemingly missing blob - was it deleted during the rebuild?",
                );
                return Ok(());
            }

            cloned_proxy
                .index_blob(&key, info_maybe.unwrap(), &cloned_node_id)
                .await?;

            tracing::debug!("rebuilt {}", &key);
            Ok(())
        }
    }))
    .buffer_unordered(2)
    .collect::<Vec<Result<()>>>()
    .await;

    // Step 3 - Catch any errors.
    puts.into_iter().collect::<Result<Vec<()>>>()?;

    // Step 4 - Tell the directory that we're done pushing.
    proxy
        .rebuild_complete(&parameters.storage_node_name)
        .await?;

    tracing::info!("rebuild complete");
    Ok(())
}
