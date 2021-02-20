use anyhow::Result;
use clap::Clap;
use menmos_client::Client;
use rood::cli::OutputManager;

mod delete;
mod download;
mod push;
mod query;

#[derive(Clap)]
pub struct Root {
    /// Whether to use verbose output.
    #[clap(short = 'v', long = "verbose", global = true)]
    verbose: bool,

    /// The config profile to use.
    #[clap(
        long = "profile",
        short = 'p',
        default_value = "default",
        global = true
    )]
    profile: String,

    /// The max number of retries for a given request..
    #[clap(short = 'r', long = "retries")]
    max_retry_count: Option<usize>,

    #[clap(subcommand)]
    command: Command,
}

impl Root {
    pub async fn run(self) -> Result<()> {
        let cli = OutputManager::new(self.verbose);

        service::config::load_or_create(cli.clone())?;
        let mut client_builder = Client::builder().with_profile(&self.profile);

        if let Some(max_retry_count) = self.max_retry_count {
            client_builder = client_builder.with_max_retry_count(max_retry_count);
        }

        let client = client_builder.build()?;

        match self.command {
            Command::Delete(cmd) => cmd.run(cli, client).await?,
            Command::Push(cmd) => cmd.run(cli, client).await?,
            Command::Query(cmd) => cmd.run(cli, client).await?,
            Command::Download(cmd) => cmd.run(cli, client).await?,
        }

        Ok(())
    }
}

#[derive(Clap)]
enum Command {
    /// Delete a blob from a menmos cluster.
    #[clap(name = "delete")]
    Delete(delete::DeleteCommand),

    /// Download a blob to disk.
    #[clap(name = "download")]
    Download(download::DownloadCommand),

    /// Push a file or directory to a menmos cluster.
    #[clap(name = "push")]
    Push(push::PushCommand),

    /// Query an menmos cluster.
    #[clap(name = "query")]
    Query(query::QueryCommand),
}
