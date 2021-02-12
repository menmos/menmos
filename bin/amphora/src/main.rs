mod logging;

use std::path::PathBuf;

use amphora::{Config, Server};
use anyhow::Result;
use clap::Clap;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clap, Debug)]
#[clap(version = VERSION)]
pub struct CLIMain {
    #[clap(short = 'c', long = "cfg")]
    cfg: Option<PathBuf>,
}

impl CLIMain {
    #[tokio::main]
    pub async fn run(self) -> Result<()> {
        let cfg = Config::new(&self.cfg)?;
        logging::init_logger(&cfg.log_config_file)?;

        let server = Server::new(cfg);

        tokio::signal::ctrl_c().await?;

        server.stop().await?;

        Ok(())
    }
}

fn main() {
    let cli = CLIMain::parse();
    if let Err(e) = cli.run() {
        log::error!("fatal: {}", e);
    }
}
