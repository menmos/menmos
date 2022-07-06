use std::fs;
use std::net::IpAddr;
use std::path::PathBuf;

use anyhow::{anyhow, Result};

use config::{builder::DefaultState, Config as ConfigLoader, ConfigBuilder, Environment, File};

use serde::{Deserialize, Serialize};

const DEFAULT_SUBNET_MASK: &str = "255.255.255.0";
const DEFAULT_SERVER_PORT: i64 = 80;

const DEFAULT_KEY_LOCKS_MAX_MEMORY: i64 = 500 * 1024;
// 500kb of memory for the locks, plus whatever for the string IDs themselves.
const DEFAULT_KEY_LOCKS_LIFETIME_SECONDS: i64 = 60 * 15;
// 15 minutes.
const DEFAULT_CHECKIN_FREQUENCY_SECONDS: i64 = 20;
const DEFAULT_MOVE_REQUEST_BUFFER_SIZE: i64 = 50;

/// The IP to which the directory node should redirect
/// when referring to this storage node.
///
/// Setting `automatic` lets this storage
/// node decide which IP to use.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum RedirectIp {
    Automatic,
    Static(IpAddr),
}

impl Default for RedirectIp {
    fn default() -> Self {
        RedirectIp::Automatic
    }
}

fn default_region() -> String {
    String::from("us-east-1")
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum BlobStorageImpl {
    Directory {
        path: PathBuf,
    },
    S3 {
        bucket: String,
        #[serde(default = "default_region")]
        region: String,

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
    pub max_concurrent_calls: usize,
    pub port: u16,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RedirectSetting {
    #[serde(default = "RedirectIp::default")]
    pub ip: RedirectIp,
    pub subnet_mask: IpAddr,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NodeSetting {
    pub name: String,
    pub db_path: PathBuf,
    pub encryption_key: String,

    pub blob_storage: BlobStorageImpl,

    pub key_locks_max_memory: usize,
    pub key_locks_lifetime_seconds: u64,

    pub checkin_frequency_seconds: u64,
    pub move_request_buffer_size: usize,

    pub maximum_capacity: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub directory: DirectoryHostConfig,
    pub node: NodeSetting,
    pub server: ServerSetting,
    pub redirect: RedirectSetting,
}

impl Config {
    fn default_loader() -> Result<ConfigBuilder<DefaultState>> {
        let mut loader = ConfigLoader::builder();

        let default_config_path = dirs::config_dir()
            .ok_or_else(|| anyhow!("cannot locate config directory"))?
            .join("menmos")
            .join("config_storage")
            .with_extension("toml");

        let data_dir = dirs::data_dir()
            .ok_or_else(|| anyhow!("cannot locate data directory"))?
            .join("menmos");

        fs::create_dir_all(&data_dir)?;

        loader = loader
            .set_default("server.port", DEFAULT_SERVER_PORT)?
            .set_default("server.max_concurrent_calls", (num_cpus::get() * 2) as i32)?
            .set_default("redirect.subnet_mask", DEFAULT_SUBNET_MASK)?
            .set_default(
                "server.certificate_storage_path",
                data_dir.join("storage_certs").to_string_lossy().to_string(),
            )?
            .set_default(
                "node.db_path",
                data_dir.join("storage_db").to_string_lossy().to_string(),
            )?
            .set_default("node.key_locks_max_memory", DEFAULT_KEY_LOCKS_MAX_MEMORY)?
            .set_default(
                "node.key_locks_lifetime_seconds",
                DEFAULT_KEY_LOCKS_LIFETIME_SECONDS,
            )?
            .set_default(
                "node.checkin_frequency_seconds",
                DEFAULT_CHECKIN_FREQUENCY_SECONDS,
            )?
            .set_default(
                "node.move_request_buffer_size",
                DEFAULT_MOVE_REQUEST_BUFFER_SIZE,
            )?
            .add_source(
                File::from(default_config_path)
                    .required(false)
                    .format(config::FileFormat::Toml),
            );
        Ok(loader)
    }

    pub fn from_toml_string<S: AsRef<str>>(cfg_str: S) -> Result<Self> {
        let mut loader = Config::default_loader()?;

        loader = loader
            .add_source(File::from_str(cfg_str.as_ref(), config::FileFormat::Toml))
            .add_source(
                Environment::with_prefix("MENMOS")
                    .separator("_")
                    .try_parsing(true),
            );

        let cfg: Config = loader.build()?.try_deserialize()?;

        println!(
            "Loaded configuration: \n{}",
            serde_json::to_string_pretty(&cfg)?
        );

        Ok(cfg)
    }

    pub fn new(cfg_file: &Option<PathBuf>) -> Result<Self> {
        let mut loader = Config::default_loader()?;

        if let Some(cfg) = cfg_file {
            loader = loader.add_source(File::from(cfg.as_ref()).required(false));
        }

        loader = loader.add_source(
            Environment::with_prefix("MENMOS")
                .separator("_")
                .try_parsing(true),
        );

        let cfg: Config = loader.build()?.try_deserialize()?;

        Ok(cfg)
    }
}
