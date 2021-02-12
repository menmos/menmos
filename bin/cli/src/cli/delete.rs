use anyhow::Result;
use clap::Clap;
use client::Client;
use rood::cli::OutputManager;

#[derive(Clap)]
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
    pub async fn run(self, cli: OutputManager, client: Client) -> Result<()> {
        cli.step(format!("Delete {} blobs", self.blob_ids.len()));

        let confirmed = if self.yes {
            true
        } else {
            cli.prompt_yn("Are you sure?", false)?
        };

        if !confirmed {
            cli.success("Aborted")
        } else {
            service::delete::delete(cli.push(), self.blob_ids, self.concurrency, client).await?;
            cli.success("Done");
        }

        Ok(())
    }
}
