use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use futures::{StreamExt, TryStreamExt};
use menmos::{Menmos, UploadRequest};
use rood::cli::OutputManager;

use crate::util;

pub async fn all(
    cli: OutputManager,
    client: Menmos,
    paths: Vec<PathBuf>,
    tags: Vec<String>,
    meta: Vec<String>,
    concurrency: usize,
    parent_id: Option<String>,
) -> Result<u64> {
    let count_rc = Arc::new(AtomicU64::new(0));
    let meta_map = util::convert_meta_vec_to_map(meta)?;

    let upload_requests = paths
        .into_iter()
        .map(|path| UploadRequest {
            path,
            metadata: meta_map.clone(),
            tags: tags.clone(),
            parent_id: parent_id.clone(),
        })
        .collect::<Vec<_>>();

    client
        .push_files(upload_requests)
        .into_stream()
        .for_each_concurrent(concurrency, |push_result| {
            let cli = cli.clone();
            let count = count_rc.clone();
            async move {
                match push_result {
                    Ok(pushed_item) => {
                        count.fetch_add(1, Ordering::SeqCst);
                        cli.success(format!(
                            "{:?} => {}",
                            pushed_item.source_path, pushed_item.blob_id
                        ))
                    }
                    Err(e) => cli.error(e.to_string()),
                }
            }
        })
        .await;

    Ok(count_rc.load(Ordering::SeqCst))
}
