use std::{
    fs,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};

use config::{builder::DefaultState, Config as ConfigLoader, ConfigBuilder, Environment, File};

use anyhow::{anyhow, Result};

use interface::RoutingAlgorithm;

use serde::{Deserialize, Serialize};

const DEFAULT_DNS_NB_OF_CONCURRENT_QUERIES: i64 = 40;
const DEFAULT_DNS_LISTEN_ADDRESS: &str = "0.0.0.0:53";
const DEFAULT_HTTP_PORT: i64 = 80;
const DEFAULT_HTTPS_PORT: i64 = 443;

#[derive(Clone, Deserialize, Serialize)]
pub struct DnsParameters {
    pub host_name: String, // The domain name of *this* node (for the example below, this would be "dir.storage.com").
    pub root_domain: String, // The domain for which to generate the wildcard cert (e.g. if you want a cert for "*.storage.com" and a directory node on "dir.storage.com", put "storage.com" here)
    pub public_ip: IpAddr,
    pub listen_address: SocketAddr,

    pub nb_of_concurrent_requests: usize,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct HttpsParameters {
    pub http_port: u16,
    pub https_port: u16,
    pub certificate_storage_path: PathBuf,
    pub letsencrypt_email: String,

    #[serde(default = "LetsEncryptUrl::default")]
    pub letsencrypt_url: LetsEncryptUrl,

    pub dns: DnsParameters,
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LetsEncryptUrl {
    Production,
    Staging,
}

impl Default for LetsEncryptUrl {
    fn default() -> Self {
        LetsEncryptUrl::Production
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct HttpParameters {
    pub port: u16,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
pub enum ServerSetting {
    Http(HttpParameters),
    Https(HttpsParameters),
}

#[derive(Clone, Deserialize, Serialize)]
pub struct NodeSetting {
    pub db_path: PathBuf,
    pub admin_password: String, // This will be used until we desing & implement proper multi-user support.
    pub encryption_key: String, // TODO: Generate instead?
    pub routing_algorithm: RoutingAlgorithm,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct Config {
    pub server: ServerSetting,
    pub node: NodeSetting,
}

impl Config {
    fn default_loader() -> Result<ConfigBuilder<DefaultState>> {
        let loader = ConfigLoader::builder();

        let default_config_path = dirs::config_dir()
            .ok_or_else(|| anyhow!("cannot locate config directory"))?
            .join("menmos")
            .join("config_directory")
            .with_extension("toml");

        let data_dir = dirs::data_dir()
            .ok_or_else(|| anyhow!("cannot locate data directory"))?
            .join("menmos");

        fs::create_dir_all(&data_dir)?;

        Ok(loader
            .set_default(
                "server.dns.nb_of_concurrent_requests",
                DEFAULT_DNS_NB_OF_CONCURRENT_QUERIES,
            )?
            .set_default("server.dns.listen_address", DEFAULT_DNS_LISTEN_ADDRESS)?
            .set_default("server.http_port", DEFAULT_HTTP_PORT)?
            .set_default("server.https_port", DEFAULT_HTTPS_PORT)?
            .set_default("server.port", DEFAULT_HTTP_PORT)?
            .set_default(
                "node.db_path",
                data_dir
                    .join("directory_db")
                    .to_string_lossy()
                    .to_string()
                    .as_ref(),
            )?
            .set_default("node.routing_algorithm", "round_robin")?
            .set_default(
                "server.certificate_storage_path",
                data_dir
                    .join("directory_certs")
                    .to_string_lossy()
                    .to_string(),
            )?
            .add_source(
                File::from(default_config_path)
                    .required(false)
                    .format(config::FileFormat::Toml),
            ))
    }

    pub fn from_toml_string<S: AsRef<str>>(cfg_string: S) -> Result<Config> {
        let mut loader = Config::default_loader()?;
        loader = loader.add_source(
            File::from_str(cfg_string.as_ref(), config::FileFormat::Toml).required(false),
        );

        let cfg: Config = loader.build()?.try_deserialize()?;

        println!(
            "Loaded configuration: \n{}",
            serde_json::to_string_pretty(&cfg)?
        );

        Ok(cfg)
    }

    pub fn from_file(cfg_file: &Option<PathBuf>) -> Result<Config> {
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
