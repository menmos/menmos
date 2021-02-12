use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use snafu::{ResultExt, Snafu};

pub const CONFIG_DIR_NAME: &str = "menmos";

#[derive(Debug, Snafu)]
pub enum ProfileError {
    MissingConfigDirectory,
    ConfigDirectoryCreationError { source: io::Error },
    ConfigReadError { source: io::Error },
    ConfigDeserializeError { source: toml::de::Error },
    ConfigSerializeError { source: toml::ser::Error },
    FileCreateError { source: io::Error },
    FileWriteError { source: io::Error },
}

type Result<T> = std::result::Result<T, ProfileError>;

fn get_config_path() -> Result<PathBuf> {
    let root_config_dir = dirs::config_dir().ok_or(ProfileError::MissingConfigDirectory)?;

    let cfg_dir_path = root_config_dir.join(CONFIG_DIR_NAME);
    if !cfg_dir_path.exists() {
        fs::create_dir_all(&cfg_dir_path).context(ConfigDirectoryCreationError)?;
    }

    Ok(cfg_dir_path.join("client").with_extension("toml"))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Profile {
    pub host: String,
    pub password: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Config {
    pub profiles: HashMap<String, Profile>,
}

impl Config {
    pub fn load() -> Result<Self> {
        let config_file = get_config_path()?;

        let cfg = if config_file.exists() {
            let buf = fs::read(config_file).context(ConfigReadError)?;
            toml::from_slice(&buf).context(ConfigDeserializeError)?
        } else {
            Config::default()
        };

        Ok(cfg)
    }

    pub fn add<S: Into<String>>(&mut self, name: S, profile: Profile) -> Result<()> {
        self.profiles.insert(name.into(), profile);

        let config_file = get_config_path()?;
        let encoded = toml::to_vec(&self.profiles).context(ConfigSerializeError)?;
        let mut f = fs::File::create(config_file).context(FileCreateError)?;
        f.write_all(&encoded).context(FileWriteError)?;
        Ok(())
    }
}
