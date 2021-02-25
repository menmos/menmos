use std::path::PathBuf;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use xecute::Daemon;

use crate::{Config, Server};

pub struct AmphoraDaemon {
    server_handle: Option<Server>,
}

impl AmphoraDaemon {
    pub fn new() -> Self {
        Self {
            server_handle: None,
        }
    }
}

#[async_trait]
impl Daemon for AmphoraDaemon {
    type Config = Config;

    fn load_config(&self, path_maybe: &Option<PathBuf>) -> Result<Self::Config> {
        Config::new(path_maybe)
    }

    async fn start(&mut self, cfg: Self::Config) -> Result<()> {
        let server = Server::new(cfg);
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
