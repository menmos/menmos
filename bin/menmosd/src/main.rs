mod logging;

use std::path::PathBuf;

use anyhow::Result;

use clap::Clap;

use menmos::{Config, Server};

#[tokio::main]
async fn main_loop(cfg: &Option<PathBuf>) -> Result<()> {
    let cfg = Config::new(cfg)?;

    logging::init_logger(&cfg.log_config_file)?;

    let s = Server::new(cfg.clone(), menmos::make_node(&cfg)?).await?;
    tokio::signal::ctrl_c().await?;
    s.stop().await.unwrap();

    Ok(())
}

#[derive(Clap, Debug)]
pub struct CLIMain {
    #[clap(long = "cfg")]
    cfg: Option<PathBuf>,
}

impl CLIMain {
    pub fn run(self) -> Result<()> {
        main_loop(&self.cfg)
    }
}

fn main() {
    let c = CLIMain::parse();
    if let Err(e) = c.run() {
        log::error!("fatal: {}", e);
    }
}
