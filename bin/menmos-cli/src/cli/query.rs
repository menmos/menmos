use anyhow::Result;
use clap::Clap;
use menmos_client::Client;
use rood::cli::OutputManager;

#[derive(Clap)]
pub struct QueryCommand {
    /// The query expression.
    expression: Option<String>,

    /// The offset from which to start fetching results.
    #[clap(long = "from", short = 'f', default_value = "0")]
    from: usize,

    /// The maximum number of results to fetch.
    #[clap(long = "size", short = 's', default_value = "10")]
    size: usize,
}

impl QueryCommand {
    pub async fn run(self, cli: OutputManager, client: Client) -> Result<()> {
        service::query::execute(cli, self.expression, self.from, self.size, client).await?;
        Ok(())
    }
}
