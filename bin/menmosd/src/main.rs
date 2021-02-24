mod logging;

use std::path::PathBuf;

use anyhow::Result;

use clap::Clap;

use menmosd::{Config, Server};

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main_loop(cfg: &Option<PathBuf>) -> Result<()> {
    let cfg = match Config::from_file(cfg) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("error loading configuration: {}", e);
            return Err(e);
        }
    };

    logging::init_logger(&cfg.log_config_file)?;

    let s = Server::new(cfg.clone(), menmosd::make_node(&cfg)?).await?;
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
