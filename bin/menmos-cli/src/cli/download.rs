use std::{
    io::{self, BufRead},
    path::PathBuf,
};

use anyhow::{anyhow, Result};
use clap::Clap;
use futures::StreamExt;
use menmos_client::Client;
use rood::cli::OutputManager;
use tokio::fs;
use tokio::io::AsyncWriteExt;

#[derive(Clap)]
pub struct DownloadCommand {
    /// The IDs of the blobs to download.
    blob_ids: Vec<String>,

    /// The directory to which to save the files.
    #[clap(long = "out", short = 'o')]
    dst_dir: Option<PathBuf>,
}

impl DownloadCommand {
    pub async fn run(self, cli: OutputManager, client: Client) -> Result<()> {
        let blob_ids = if self.blob_ids.is_empty() {
            // Get from stdin
            let stdin = io::stdin();
            stdin.lock().lines().filter_map(|l| l.ok()).collect()
        } else {
            self.blob_ids
        };

        // TODO: Extract to service & parallelize.
        cli.step(format!("Downloading {} files...", &blob_ids.len()));

        let pushed = cli.push();
        for blob_id in blob_ids.into_iter() {
            pushed.step(format!("Downloading blob {}...", &blob_id));
            let meta = match client.get_meta(&blob_id).await? {
                Some(b) => b,
                None => {
                    cli.error("404 - Blob not found");
                    continue;
                }
            };

            let stream = client.get_file(&blob_id).await?;
            let mut stream_pin = Box::pin(stream);

            let file_path = match &self.dst_dir {
                Some(d) => d.join(&meta.name),
                None => PathBuf::from(meta.name),
            };

            let mut f = fs::File::create(&file_path).await?;

            while let Some(chunk) = stream_pin.next().await {
                match chunk {
                    Ok(c) => f.write_all(c.as_ref()).await?,
                    Err(e) => {
                        fs::remove_file(&file_path).await?;
                        return Err(anyhow!("{}", e.to_string()));
                    }
                }
            }
            pushed.success("Done");
        }

        cli.success("Done.");
        Ok(())
    }
}
