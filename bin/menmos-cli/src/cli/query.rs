use anyhow::Result;
use clap::Parser;
use menmos::Menmos;
use rood::cli::OutputManager;

#[derive(Parser)]
pub struct QueryCommand {
    /// The query expression.
    expression: Option<String>,

    /// The offset from which to start fetching results.
    #[clap(long = "from", short = 'f', default_value = "0")]
    from: usize,

    /// The maximum number of results to fetch.
    #[clap(long = "size", short = 's', default_value = "10")]
    size: usize,

    /// Whether to enumerate all results .
    #[clap(long = "all", short = 'a')]
    all: bool,
}

impl QueryCommand {
    #[tracing::instrument(skip(self, cli, client))]
    pub async fn run(self, cli: OutputManager, client: Menmos) -> Result<()> {
        service::query::execute(cli, self.expression, self.from, self.size, self.all, client)
            .await?;
        Ok(())
    }
}
