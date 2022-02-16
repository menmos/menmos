use std::path::PathBuf;
use std::time::Instant;

use anyhow::Result;
use clap::Parser;
use menmos::Menmos;
use rood::cli::OutputManager;

#[derive(Parser)]
pub struct PushCommand {
    /// The maximum number of concurrent requests.
    #[clap(long = "concurrency", short = 'c', default_value = "4")]
    concurrency: usize,

    /// A field:value pair to add to all content uploaded in this run.
    #[clap(long = "field", short = 'f')]
    fields: Vec<String>,

    /// Tags to add to all content uploaded in this run.
    #[clap(long = "tag", short = 't')]
    tags: Vec<String>,

    /// The path of the file(s) or directory to upload.
    paths: Vec<PathBuf>,
}

impl PushCommand {
    pub async fn run(self, cli: OutputManager, client: Menmos) -> Result<()> {
        cli.step("Upload started");

        let start = Instant::now();
        let count = service::push::all(
            cli.push(),
            client,
            self.paths,
            self.tags,
            self.fields,
            self.concurrency,
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
