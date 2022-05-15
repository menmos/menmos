use anyhow::{anyhow, Result};
use clap::Parser;
use menmos::Menmos;
use rood::cli::OutputManager;

mod delete;
mod download;
mod meta;
mod nodes;
mod push;
mod query;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser)]
#[clap(version = VERSION, author = "Menmos Team")]
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

        service::config::load_or_create(cli.clone())?;
        let client_builder = Menmos::builder(&self.profile);
        let client = client_builder.build().await.map_err(|e| anyhow!("{e}"))?;

        match self.command {
            Command::Delete(cmd) => cmd.run(cli, client).await?,
            Command::Push(cmd) => cmd.run(cli, client).await?,
            Command::Query(cmd) => cmd.run(cli, client).await?,
            Command::Download(cmd) => cmd.run(cli, client).await?,
            Command::Nodes(cmd) => cmd.run(cli, client).await?,
            Command::Meta(cmd) => cmd.run(cli, client).await?,
        }

        Ok(())
    }
}

#[derive(Parser)]
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

    /// List the storage nodes of a menmos cluster.
    #[clap(name = "nodes")]
    Nodes(nodes::ListStorageNodesCommand),

    /// List and query the metadata of a menmos cluster.
    Meta(meta::ListMetadataCommand),
}
