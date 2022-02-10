use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use snafu::prelude::*;

pub const CONFIG_DIR_NAME: &str = "menmos";

#[derive(Debug, Snafu)]
pub enum ProfileError {
    #[snafu(display("couldn't find config directory"))]
    MissingConfigDirectory,

    #[snafu(display("failed to create config directory: {}", source))]
    ConfigDirectoryCreateError { source: std::io::Error },

    #[snafu(display("failed to read config: {}", source))]
    ConfigReadError { source: std::io::Error },

    #[snafu(display("failed to write config: {}", source))]
    ConfigWriteError { source: std::io::Error },

    #[snafu(display("failed to deserialize config: {}", source))]
    ConfigDeserializeError { source: toml::de::Error },

    #[snafu(display("failed to serialize config: {}", source))]
    ConfigSerializeError { source: toml::ser::Error },
}

type Result<T> = std::result::Result<T, ProfileError>;

fn get_config_path() -> Result<PathBuf> {
    let root_config_dir = dirs::config_dir().context(MissingConfigDirectorySnafu)?;

    let cfg_dir_path = root_config_dir.join(CONFIG_DIR_NAME);
    if !cfg_dir_path.exists() {
        fs::create_dir_all(&cfg_dir_path).context(ConfigDirectoryCreateSnafu)?;
    }

    Ok(cfg_dir_path.join("client").with_extension("toml"))
}

/// A client profile containing credentials to a menmos cluster.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Profile {
    pub host: String,
    pub username: String,
    pub password: String,
}

/// A client configuration, as stored on disk.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Config {
    /// The configuration profiles set by the user.
    pub profiles: HashMap<String, Profile>,
}

impl Config {
    pub fn load() -> Result<Self> {
        let config_file = get_config_path()?;

        let cfg = if config_file.exists() {
            let buf = fs::read(config_file).context(ConfigReadSnafu)?;
            toml::from_slice(&buf).context(ConfigDeserializeSnafu)?
        } else {
            Config::default()
        };

        Ok(cfg)
    }

    pub fn add<S: Into<String>>(&mut self, name: S, profile: Profile) -> Result<()> {
        self.profiles.insert(name.into(), profile);

        let config_file = get_config_path()?;
        let encoded = toml::to_vec(&self).context(ConfigSerializeSnafu)?;
        let mut f = fs::File::create(config_file).context(ConfigWriteSnafu)?;
        f.write_all(&encoded).context(ConfigWriteSnafu)?;
        Ok(())
    }
}
