mod logging;

use std::path::PathBuf;

use anyhow::Result;

use clap::Clap;

use menmosd::{Config, Server};

use tokio::runtime;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const MINIMUM_WORKER_THREAD_COUNT: usize = 6;

fn worker_thread_count() -> usize {
    let core_count = num_cpus::get();
    core_count.max(MINIMUM_WORKER_THREAD_COUNT)
}

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
#[clap(version = VERSION)]
pub struct CLIMain {
    #[clap(long = "cfg")]
    cfg: Option<PathBuf>,
}

impl CLIMain {
    pub fn run(self) -> Result<()> {
        let rt = runtime::Builder::new_multi_thread()
            .worker_threads(worker_thread_count())
            .build()?;

        rt.block_on(async { main_loop(&self.cfg).await })
    }
}

fn main() {
    let c = CLIMain::parse();
    if let Err(e) = c.run() {
        log::error!("fatal: {}", e);
    }
}
