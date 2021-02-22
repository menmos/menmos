use std::sync::Arc;

use anyhow::Result;
use futures::StreamExt;
use interface::BlobMeta;

use super::directory_proxy::DirectoryProxy;

pub struct Params {
    pub storage_node_name: String,
    pub directory_host_url: String,
    pub directory_host_port: usize,
    pub directory_node_password: String,
}

pub async fn execute(
    parameters: Params,
    proxy: Arc<DirectoryProxy>,
    db: Arc<sled::Db>,
) -> Result<()> {
    log::info!("starting node rebuild");

    // Step 1 - Get a set of keys to push (so we dont re-push documents that are indexed during the rebuild).
    let keys: Vec<_> = db
        .iter()
        .filter_map(|r| r.ok())
        .map(|(k, _v)| String::from_utf8_lossy(k.as_ref()).to_string())
        .collect();

    // Step 2 - Push all those keys (and their meta) back to the directory.
    let puts = futures::stream::iter(keys.into_iter().map(|key| {
        let cloned_proxy = proxy.clone();
        let cloned_db = db.clone();
        let cloned_node_id = parameters.storage_node_name.clone();
        async move {
            let meta_maybe = cloned_db.get(key.as_bytes())?;
            if meta_maybe.is_none() {
                log::warn!(
                    "seemingly missing blob: {} - was it deleted during the rebuild?",
                    &key
                );
                return Ok(());
            }

            let meta: BlobMeta = bincode::deserialize(meta_maybe.unwrap().as_ref())?;
            cloned_proxy.index_blob(&key, meta, &cloned_node_id).await?;
            log::info!("rebuilt {}", &key);
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

    log::info!("rebuild complete");
    Ok(())
}
