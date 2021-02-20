use std::fs;
use std::net::IpAddr;
use std::path::PathBuf;

use anyhow::{anyhow, Result};

use config::{Config as ConfigLoader, Environment, File};

use serde::{Deserialize, Serialize};

const DEFAULT_SUBNET_MASK: &str = "255.255.255.0";
const DEFAULT_SERVER_PORT: i64 = 80;

const DEFAULT_KEY_LOCKS_MAX_MEMORY: i64 = 500 * 1024; // 500kb of memory for the locks, plus whatever for the string IDs themselves.
const DEFAULT_KEY_LOCKS_LIFETIME_SECONDS: i64 = 60 * 15; // 15 minutes.

/// The IP to which the directory node should redirect
/// when referring to this storage node.
///
/// Setting `automatic` lets this storage
/// node decide which IP to use.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum RedirectIP {
    Automatic,
    Static(IpAddr),
}

impl Default for RedirectIP {
    fn default() -> Self {
        RedirectIP::Automatic
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum BlobStorageImpl {
    Directory {
        path: PathBuf,
    },
    S3 {
        bucket: String,
        cache_path: PathBuf,
        cache_size: usize,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DirectoryHostConfig {
    pub url: String,
    pub port: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ServerSetting {
    pub certificate_storage_path: PathBuf,
    pub subnet_mask: IpAddr, // TODO: Move subnet mask in redirect instructions since it's used only for that.
    pub port: u16,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NodeSetting {
    pub name: String,
    pub db_path: PathBuf,
    pub admin_password: String,
    pub encryption_key: String,

    #[serde(default = "RedirectIP::default")]
    pub redirect_ip: RedirectIP,

    pub blob_storage: BlobStorageImpl,

    pub key_locks_max_memory: usize,
    pub key_locks_lifetime_seconds: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub directory: DirectoryHostConfig,
    pub node: NodeSetting,
    pub server: ServerSetting,
    pub log_config_file: Option<PathBuf>,
}

impl Config {
    fn default_loader() -> Result<ConfigLoader> {
        let mut loader = ConfigLoader::new();

        let default_config_path = dirs::config_dir()
            .ok_or_else(|| anyhow!("cannot locate config directory"))?
            .join("menmos")
            .join("config_storage")
            .with_extension("toml");

        let data_dir = dirs::data_dir()
            .ok_or_else(|| anyhow!("cannot locate data directory"))?
            .join("menmos");

        fs::create_dir_all(&data_dir)?;

        loader.set_default("server.port", DEFAULT_SERVER_PORT)?;
        loader.set_default("server.subnet_mask", DEFAULT_SUBNET_MASK)?;
        loader.set_default(
            "server.certificate_storage_path",
            data_dir.join("storage_certs").to_string_lossy().to_string(),
        )?;

        loader.set_default(
            "node.db_path",
            data_dir.join("storage_db").to_string_lossy().to_string(),
        )?;

        loader.set_default("node.key_locks_max_memory", DEFAULT_KEY_LOCKS_MAX_MEMORY)?;
        loader.set_default(
            "node.key_locks_lifetime_seconds",
            DEFAULT_KEY_LOCKS_LIFETIME_SECONDS,
        )?;

        loader.merge(
            File::from(default_config_path)
                .required(false)
                .format(config::FileFormat::Toml),
        )?;
        Ok(loader)
    }

    pub fn from_toml_string<S: AsRef<str>>(cfg_str: S) -> Result<Self> {
        let mut loader = Config::default_loader()?;

        loader.merge(File::from_str(cfg_str.as_ref(), config::FileFormat::Toml))?;
        loader.merge(
            Environment::with_prefix("MENMOS")
                .separator("_")
                .try_parsing(true),
        )?;

        let cfg: Config = loader.try_into()?;

        println!(
            "Loaded configuration: \n{}",
            serde_json::to_string_pretty(&cfg)?
        );

        Ok(cfg)
    }

    pub fn new(cfg_file: &Option<PathBuf>) -> Result<Self> {
        let mut loader = Config::default_loader()?;

        if let Some(cfg) = cfg_file {
            loader.merge(File::from(cfg.as_ref()).required(false))?;
        }

        loader.merge(
            Environment::with_prefix("MENMOS")
                .separator("_")
                .try_parsing(true),
        )?;

        let cfg: Config = loader.try_into()?;

        println!(
            "Loaded configuration: \n{}",
            serde_json::to_string_pretty(&cfg)?
        );

        Ok(cfg)
    }
}
