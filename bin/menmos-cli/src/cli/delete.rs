use std::io::{self, BufRead};

use anyhow::Result;
use clap::Parser;
use menmos::Menmos;
use rood::cli::OutputManager;

#[derive(Parser)]
pub struct DeleteCommand {
    /// The maximum number of concurrent requests.
    #[clap(long = "concurrency", short = 'c', default_value = "4")]
    concurrency: usize,

    #[clap(long = "yes", short = 'y')]
    yes: bool,

    /// The IDs of the blobs to delete.
    blob_ids: Vec<String>,
}

impl DeleteCommand {
    pub async fn run(self, cli: OutputManager, client: Menmos) -> Result<()> {
        let blob_ids = if self.blob_ids.is_empty() {
            // Get from stdin
            let stdin = io::stdin();
            stdin.lock().lines().filter_map(|l| l.ok()).collect()
        } else {
            self.blob_ids.clone()
        };

        cli.step(format!("Delete {} blobs", blob_ids.len()));

        let confirmed = if self.yes {
            true
        } else {
            cli.prompt_yn("Are you sure?", false)?
        };

        if !confirmed {
            cli.success("Aborted")
        } else {
            service::delete::delete(cli.push(), blob_ids, self.concurrency, client).await?;
            cli.success("Done");
        }

        Ok(())
    }
}
