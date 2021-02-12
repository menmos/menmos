use anyhow::{anyhow, Result};
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

    #[clap(subcommand)]
    command: Command,
}

impl Root {
    pub async fn run(self) -> Result<()> {
        let cli = OutputManager::new(self.verbose);

        let cfg = service::config::load_or_create(cli.clone())?;
        let profile = cfg
            .profiles
            .get(&self.profile)
            .cloned()
            .ok_or_else(|| anyhow!("missing profile [{}]", &self.profile))?;

        let client = Client::new(profile.host, profile.password)?;

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
    Delete(delete::DeleteCommand),

    /// Download a blob to disk.
    Download(download::DownloadCommand),

    /// Push a file or directory to a menmos cluster.
    #[clap(name = "push")]
    Push(push::PushCommand),

    /// Query an menmos cluster.
    #[clap(name = "query")]
    Query(query::QueryCommand),
}
