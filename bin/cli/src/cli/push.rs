use std::path::PathBuf;
use std::time::Instant;

use anyhow::Result;
use clap::Clap;
use client::Client;
use rood::cli::OutputManager;

#[derive(Clap)]
pub struct PushCommand {
    /// The maximum number of concurrent requests.
    #[clap(long = "concurrency", short = 'c', default_value = "4")]
    concurrency: usize,

    #[clap(long = "retry", default_value = "20")]
    max_retry: usize,

    /// A key/value pair to add to all content uploaded in this run.
    #[clap(long = "meta", short = 'm')]
    meta: Vec<String>,

    /// Tags to add to all content uploaded in this run.
    #[clap(long = "tag", short = 't')]
    tags: Vec<String>,

    /// The path of the file(s) or directory to upload.
    paths: Vec<PathBuf>,
}

impl PushCommand {
    pub async fn run(self, cli: OutputManager, client: Client) -> Result<()> {
        cli.step("Upload started");

        let start = Instant::now();
        let count = service::push::all(
            cli.push(),
            client,
            self.paths,
            self.tags,
            self.meta,
            self.concurrency,
            self.max_retry,
            None,
        )
        .await?;

        let elapsed = Instant::now().duration_since(start);
        cli.success(format!(
            "Uploaded {} items in {} seconds",
            count,
            elapsed.as_secs()
        ));

        Ok(())
    }
}
