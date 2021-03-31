use std::path::PathBuf;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use xecute::Daemon;

use crate::{Config, Server};

pub struct MenmosdDaemon {
    server_handle: Option<Server>,
}

impl MenmosdDaemon {
    pub fn new() -> Self {
        Self {
            server_handle: None,
        }
    }
}

impl Default for MenmosdDaemon {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Daemon for MenmosdDaemon {
    type Config = Config;

    fn load_config(&self, path_maybe: &Option<PathBuf>) -> Result<Self::Config> {
        Config::from_file(path_maybe)
    }

    async fn start(&mut self, cfg: Self::Config) -> Result<()> {
        let node = crate::make_node(&cfg)?;
        let server = Server::new(cfg, node).await?;
        self.server_handle = Some(server);
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        let handle = self
            .server_handle
            .take()
            .ok_or_else(|| anyhow!("missing server handle"))?;

        handle.stop().await
    }
}
