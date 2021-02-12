use std::sync::Arc;

use anyhow::Result;
use futures::StreamExt;
use menmos_client::Client;
use rood::cli::OutputManager;

pub async fn delete(
    cli: OutputManager,
    blob_ids: Vec<String>,
    concurrency: usize,
    client: Client,
) -> Result<()> {
    let client_arc = Arc::from(client);

    let deletions = futures::stream::iter(blob_ids.into_iter().map(|blob_id| {
        let client_cloned = client_arc.clone();
        let cli_cloned = cli.clone();
        async move {
            client_cloned.delete(blob_id.clone()).await?;
            cli_cloned.success(format!("Deleted blob {}", &blob_id));
            Ok(())
        }
    }))
    .buffer_unordered(concurrency)
    .collect::<Vec<Result<()>>>()
    .await;

    // Catch any errors.
    deletions.into_iter().collect::<Result<Vec<()>>>()?;

    Ok(())
}
