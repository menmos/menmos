mod cli;

use clap::Clap;
use rood::cli::OutputManager;

#[tokio::main]
async fn main() {
    if let Err(e) = cli::Root::parse().run().await {
        OutputManager::new(false).error(&e.to_string());
    }
}
