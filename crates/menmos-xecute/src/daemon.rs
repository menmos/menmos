use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use clap::{App, Arg};
use tokio::runtime;
use tracing::instrument;

use crate::logging;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const MINIMUM_WORKER_THREAD_COUNT: usize = 6;

#[async_trait]
pub trait Daemon {
    type Config;

    fn load_config(&self, path_maybe: &Option<PathBuf>) -> Result<Self::Config>;

    async fn start(&mut self, cfg: Self::Config) -> Result<()>;
    async fn stop(&mut self) -> Result<()>;
}

fn worker_thread_count() -> usize {
    let core_count = num_cpus::get();
    core_count.max(MINIMUM_WORKER_THREAD_COUNT)
}

#[instrument(level = "trace")]
fn init_runtime() -> Result<runtime::Runtime> {
    let threads = worker_thread_count();
    let rt = runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .worker_threads(threads)
        .build()?;
    tracing::trace!(threads = threads, "initialized tokio runtime");
    Ok(rt)
}

fn main_loop<D: Daemon>(
    cfg_path: Option<PathBuf>,
    log_config: Option<PathBuf>,
    mut daemon: D,
) -> Result<()> {
    logging::init_logger(&log_config)?;
    let cfg = daemon.load_config(&cfg_path)?;

    let rt = init_runtime()?;
    rt.block_on(async move {
        daemon.start(cfg).await?;
        tokio::signal::ctrl_c().await?;
        daemon.stop().await?;

        Ok(())
    })
}

pub struct DaemonProcess {}

impl DaemonProcess {
    pub fn start<D: Daemon>(name: &str, about: &str, daemon: D) {
        let matches = App::new(name)
            .version(VERSION)
            .about(about)
            .arg(
                Arg::new("config")
                    .short('c')
                    .long("cfg")
                    .value_name("FILE")
                    .about("Sets a config file")
                    .takes_value(true),
            )
            .arg(
                Arg::new("log_config")
                    .short('l')
                    .long("log")
                    .about("Sets the log config file")
                    .takes_value(true),
            )
            .get_matches();

        let cfg: Option<PathBuf> = matches.value_of_t("config").ok();

        let log_config: Option<PathBuf> = matches.value_of_t("log_config").ok();

        if let Err(e) = main_loop(cfg, log_config, daemon) {
            tracing::error!("fatal: {}", e);
        }
    }
}
