use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use clap::Clap;
use env_logger::Env;

use omnifs::{Config, OmniFS};

#[derive(Clap)]
pub struct CLIMain {
    #[clap(long = "cfg", short = 'c')]
    cfg: PathBuf,
}

impl CLIMain {
    pub async fn run(self) -> Result<()> {
        let options = ["-o", "fsname=menmos", "blksize=512"]
            .iter()
            .map(|s| OsString::from(&s))
            .collect::<Vec<OsString>>();

        let buf = fs::read(self.cfg)?;
        let cfg: Config = serde_json::from_slice(&buf)?;

        let mount_dir = cfg.mount.mount_point.clone();
        fs::create_dir_all(&mount_dir)?;

        let filesystem = OmniFS::new(cfg.mount).await?;
        let cloned_opt = options.clone();

        if let Err(e) = async_fuse::mount(filesystem, mount_dir, &cloned_opt) {
            log::error!("error: {}", e);
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    env_logger::init_from_env(Env::default().default_filter_or("info"));
    let c = CLIMain::parse();
    if let Err(e) = c.run().await {
        log::error!("unhandled error: {}", e);
    }
}
