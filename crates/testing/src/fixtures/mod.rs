use std::io::Write;
use std::sync::Once;
use std::time::Duration;

use anyhow::{anyhow, Result};

use log::LevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::config::{Appender, Config as LogConfig, Root};
use menmos_client::{Client, Meta};
use menmosd::config::{HTTPParameters, ServerSetting};
use menmosd::{Config, Server};
use tempfile::{NamedTempFile, TempDir};

const DIRECTORY_PASSWORD: &str = "password";

static INIT: Once = Once::new();

fn init_logger() {
    INIT.call_once(|| {
        let stdout = ConsoleAppender::builder().build();
        let config = LogConfig::builder()
            .appender(Appender::builder().build("stdout", Box::new(stdout)))
            .build(Root::builder().appender("stdout").build(LevelFilter::Info))
            .unwrap();
        log4rs::init_config(config).unwrap();
    });
}

pub struct Menmos {
    directory: Server,
    amphorae: Vec<amphora::Server>,

    directory_port: u16,
    pub root_directory: TempDir,

    pub directory_url: String,
    pub directory_password: String,
    pub client: Client,
}

impl Menmos {
    pub async fn new() -> Result<Self> {
        init_logger();
        let root_directory = TempDir::new()?;
        let db_path = root_directory.path().join("directory_db");

        const MENMOSD_CONFIG: &str = include_str!("data/menmosd_http.toml");

        let mut cfg = Config::from_toml_string(MENMOSD_CONFIG)?;
        cfg.node.db_path = db_path;

        let port = portpicker::pick_unused_port().unwrap();
        cfg.server = ServerSetting::HTTP(HTTPParameters { port });

        let node = menmosd::make_node(&cfg)?;

        let dir_server = Server::new(cfg, node).await?;

        let directory_url = format!("http://localhost:{}", port);
        let client = Client::new(&directory_url, "admin", DIRECTORY_PASSWORD).await?;

        Ok(Self {
            directory: dir_server,
            directory_port: port,
            amphorae: Vec::new(),
            root_directory,
            directory_url,
            directory_password: DIRECTORY_PASSWORD.into(),
            client,
        })
    }

    pub async fn add_user<U: AsRef<str>, P: AsRef<str>>(
        &self,
        username: U,
        password: P,
    ) -> Result<String> {
        let tok = self
            .client
            .register(username.as_ref(), password.as_ref())
            .await?;
        Ok(tok)
    }

    pub async fn push_document<B: AsRef<[u8]>>(&self, body: B, meta: Meta) -> Result<String> {
        let tfile = NamedTempFile::new()?;
        tfile.as_file().write_all(body.as_ref())?;
        let file_path = tfile.into_temp_path();

        let blob_id = self.client.push(&file_path, meta).await?;

        Ok(blob_id)
    }

    pub async fn push_document_client<B: AsRef<[u8]>>(
        &self,
        body: B,
        meta: Meta,
        client: &Client,
    ) -> Result<String> {
        let tfile = NamedTempFile::new()?;
        tfile.as_file().write_all(body.as_ref())?;
        let file_path = tfile.into_temp_path();

        let blob_id = client.push(&file_path, meta).await?;

        Ok(blob_id)
    }

    pub async fn add_amphora<S: Into<String>>(&mut self, name: S) -> Result<()> {
        const BASE_CFG: &str = include_str!("data/amphora_http.toml");
        let mut cfg = amphora::Config::from_toml_string(BASE_CFG)?;
        let port = portpicker::pick_unused_port().unwrap();

        let name_str = name.into();

        let blob_path = self
            .root_directory
            .path()
            .join(&format!("{}-blobs", &name_str));
        let db_path = self
            .root_directory
            .path()
            .join(&format!("storage-{}-db", &name_str));

        cfg.server.port = port;
        cfg.directory.port = self.directory_port as usize;
        cfg.node.name = name_str;
        cfg.node.db_path = db_path;
        cfg.node.blob_storage = amphora::BlobStorageImpl::Directory { path: blob_path };

        let initial_node_count = self.client.list_storage_nodes().await?.storage_nodes.len();

        self.amphorae.push(amphora::Server::new(cfg));

        // Wait for the node to register itself.
        let mut iter_count = 20;
        while iter_count >= 0 {
            if self.client.list_storage_nodes().await?.storage_nodes.len() > initial_node_count {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            iter_count -= 1;
        }

        if iter_count < 0 {
            return Err(anyhow!("storage node won't come up - retries exceeded"));
        }

        Ok(())
    }

    pub async fn stop_all(self) -> Result<()> {
        // Stop the storage nodes.
        let amphorae_stop_futures: Vec<_> = self.amphorae.into_iter().map(|a| a.stop()).collect();
        let r: Result<Vec<()>> = futures::future::join_all(amphorae_stop_futures)
            .await
            .into_iter()
            .collect();
        r?;

        // Stop the directory.
        self.directory.stop().await?;

        Ok(())
    }
}
