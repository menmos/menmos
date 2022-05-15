use std::io::Write;
use std::net::IpAddr;
use std::sync::Once;
use std::time::Duration;

use anyhow::{anyhow, ensure, Result};

use interface::StorageNodeInfo;
use menmos_client::{Client, Meta};
use menmosd::config::{HttpParameters, ServerSetting};
use menmosd::{Config, Server};
use protocol::directory::storage::MoveRequest;
use tempfile::{NamedTempFile, TempDir};

const DIRECTORY_PASSWORD: &str = "password";

static INIT: Once = Once::new();

fn init_logger() {
    INIT.call_once(|| {
        xecute::logging::init_logger("menmos-test", &None).unwrap();
    });
}

pub struct Menmos {
    directory: Server,
    amphorae: Vec<amphora::Server>,
    amphora_urls: Vec<String>,

    directory_port: u16,
    pub root_directory: TempDir,

    pub directory_url: String,
    pub directory_password: String,
    pub client: Client,
    pub config: Config,
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
        cfg.server = ServerSetting::Http(HttpParameters { port });

        let node = menmosd::make_node(&cfg)?;

        let dir_server = Server::new(cfg.clone(), node).await?;

        let directory_url = format!("http://localhost:{}", port);
        let client = Client::new(&directory_url, "admin", DIRECTORY_PASSWORD).await?;

        Ok(Self {
            directory: dir_server,
            directory_port: port,
            amphorae: Vec::new(),
            amphora_urls: Vec::new(),
            root_directory,
            directory_url,
            directory_password: DIRECTORY_PASSWORD.into(),
            client,
            config: cfg,
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

    pub async fn update_document_client<B: AsRef<[u8]>>(
        &self,
        blob_id: &str,
        body: B,
        meta: Meta,
        client: &Client,
    ) -> Result<()> {
        let tfile = NamedTempFile::new()?;
        tfile.as_file().write_all(body.as_ref())?;
        let file_path = tfile.into_temp_path();
        client.update_blob(blob_id, &file_path, meta).await?;
        Ok(())
    }

    pub async fn get_move_requests_from<S: AsRef<str>>(
        &self,
        storage_node_id: S,
    ) -> Result<Vec<MoveRequest>> {
        let reqwest_client = reqwest::Client::new();
        let auth_token = menmos_auth::make_token(
            &self.config.node.encryption_key,
            menmos_auth::StorageNodeIdentity { id: "alpha".into() },
        )?;

        let url = self.directory_url.clone() + "/node/storage";

        let mock_node_info = StorageNodeInfo {
            id: String::from(storage_node_id.as_ref()),
            redirect_info: interface::RedirectInfo::Static {
                static_address: IpAddr::from([127, 0, 0, 1]),
            },
            port: 8081,
            size: 0,
            available_space: 1000 * 1000,
        };

        let req = reqwest_client
            .put(&url)
            .bearer_auth(&auth_token)
            .json(&mock_node_info)
            .build()?;

        let resp = reqwest_client.execute(req).await?;

        ensure!(
            resp.status().is_success(),
            "unexpected response status: {}",
            resp.status()
        );

        let body_bytes = resp.bytes().await?;
        let reg_response: protocol::directory::storage::RegisterResponse =
            serde_json::from_slice(body_bytes.as_ref())?;

        Ok(reg_response.move_requests)
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
        self.amphora_urls.push(format!("http://localhost:{}", port));

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

    pub async fn flush(&self) -> Result<()> {
        let auth_token = menmos_auth::make_token(
            &self.config.node.encryption_key,
            menmos_auth::UserIdentity {
                username: String::from("admin"),
                admin: true,
                blobs_whitelist: None,
            },
        )?;

        let reqwest_client = reqwest::Client::new();

        let mut urls = self.amphora_urls.clone();
        urls.push(self.directory_url.clone());

        for amphora_url in urls.iter() {
            let req = reqwest_client
                .post(&format!("{}/flush", amphora_url))
                .bearer_auth(&auth_token)
                .build()?;

            let resp = reqwest_client.execute(req).await?;
            ensure!(
                resp.status().is_success(),
                "unexpected response status: {}",
                resp.status()
            );
        }

        Ok(())
    }
}
