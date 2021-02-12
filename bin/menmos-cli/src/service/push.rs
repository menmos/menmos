use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use menmos_client::{Client, Meta, Type};
use rood::cli::OutputManager;

use crate::util;

// TODO: Refactor arguments.
async fn file<P: AsRef<Path>>(
    cli: OutputManager,
    path: P,
    client: Arc<Client>,
    max_retry: usize,
    tags: Vec<String>,
    meta_map: HashMap<String, String>,
    blob_type: Type,
    parent: Option<String>,
) -> Result<String> {
    cli.step(format!("Started upload for {:?}", path.as_ref()));

    // Retry mechanism to help with the occasional broken pipe.
    let mut error_count = 0;

    let mut meta = Meta::new(
        path.as_ref()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string(),
        blob_type.clone(),
    )
    .with_meta(
        "extension",
        path.as_ref()
            .extension()
            .map(|e| e.to_string_lossy().to_string())
            .unwrap_or_else(String::default),
    );

    if blob_type == Type::File {
        meta = meta.with_size(path.as_ref().metadata().unwrap().len())
    }

    if let Some(parent) = parent {
        meta = meta.with_parent(parent);
    }

    for tag in tags.iter() {
        meta = meta.with_tag(tag);
    }

    for (k, v) in meta_map.iter() {
        meta = meta.with_meta(k, v);
    }

    while error_count < max_retry {
        match client.push(path.as_ref(), meta.clone()).await {
            Ok(item_id) => {
                cli.success(format!("Complete {:?} => {}", path.as_ref(), &item_id));
                return Ok(item_id);
            }
            Err(e) => {
                cli.debug(format!("upload error ({}), retrying in 100ms", e));
                error_count += 1;
                tokio::time::delay_for(Duration::from_millis(100)).await;
            }
        }
    }

    Err(anyhow!("retries exceeded"))
}

fn get_file_stream(
    cli: OutputManager,
    client_arc: Arc<Client>,
    paths: Vec<PathBuf>,
    tags: Vec<String>,
    meta_map: HashMap<String, String>,
    max_retry: usize,
    parent_id: Option<String>,
) -> impl Stream<Item = Result<(Option<String>, PathBuf)>> {
    // Convert a non-recursive (stack based) directory traversal to a stream
    try_stream! {
        let mut working_stack = Vec::new();
        working_stack.extend(paths.into_iter().map(|path| (parent_id.clone(), path)));

        while !working_stack.is_empty() {
            let (parent_maybe, file_path) = working_stack.pop().unwrap();

            if file_path.is_file() {
                yield (parent_maybe, file_path); // File can be uploaded directly.
            } else {
                let directory_id = file(
                    cli.clone(),
                    file_path.clone(),
                    client_arc.clone(),
                    max_retry,
                    tags.clone(),
                    meta_map.clone(),
                    Type::Directory,
                    parent_maybe,
                )
                .await?;

                // Add this directory's children to the working stack.
                for child in file_path.read_dir()?.filter_map(|f| f.ok()) {
                    working_stack.push((Some(directory_id.clone()), child.path()));
                }

            }
        }
    }
}

pub async fn all(
    cli: OutputManager,
    client: Client,
    paths: Vec<PathBuf>,
    tags: Vec<String>,
    meta: Vec<String>,
    concurrency: usize,
    max_retry: usize,
    parent_id: Option<String>,
) -> Result<i32> {
    let client_arc = Arc::from(client);
    let mut count = 0;

    let meta_map = util::convert_meta_vec_to_map(meta)?;

    let file_stream = get_file_stream(
        cli.clone(),
        client_arc.clone(),
        paths.clone(),
        tags.clone(),
        meta_map.clone(),
        max_retry,
        parent_id.clone(),
    );

    let puts = file_stream
        .filter_map(|result| {
            let cloned_cli = cli.clone();
            async move {
                match result {
                    Ok(pair) => Some(pair),
                    Err(e) => {
                        cloned_cli.clone().error(format!("filestream error: {}", e));
                        None
                    }
                }
            }
        })
        .map(|(parent_maybe, file_path)| {
            count += 1;
            let cloned_client = client_arc.clone();
            let cloned_cli = cli.clone();
            let tags_cloned = tags.clone();
            let meta_cloned = meta_map.clone();
            async move {
                file(
                    cloned_cli,
                    file_path,
                    cloned_client,
                    max_retry,
                    tags_cloned,
                    meta_cloned,
                    Type::File,
                    parent_maybe,
                )
                .await?;
                Ok(())
            }
        })
        .buffered(concurrency)
        .collect::<Vec<Result<()>>>()
        .await;

    // Catch any errors that occurred.
    puts.into_iter().collect::<Result<Vec<()>>>()?;

    Ok(count)
}
