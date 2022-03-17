use std::path::PathBuf;

use anyhow::Result;

use config::Config;

use serde::{Deserialize, Serialize};

use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

const DEFAULT_TRACKED_CRATES: &[&str] = &[
    "menmosd",
    "amphora",
    "xecute",
    "rapidquery",
    "antidns",
    "lfan",
    "apikit",
    "menmos_auth",
    "menmos_client",
    "menmos_protocol",
    "betterstreams",
    "repository",
    "menmos-std",
    "tower_http",
];

#[cfg(debug_assertions)]
const NORMAL_CRATE_LEVEL: &str = "debug";

#[cfg(not(debug_assertions))]
const NORMAL_CRATE_LEVEL: &str = "info";

#[cfg(debug_assertions)]
const DETAILED_CRATE_LEVEL: &str = "trace";

#[cfg(not(debug_assertions))]
const DETAILED_CRATE_LEVEL: &str = "debug";

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Normal,
    Detailed,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum LogStructure {
    Preset(LogLevel),
    Explicit(Vec<String>),
}

fn default_json() -> bool {
    false
}

#[derive(Debug, Deserialize, Serialize)]
pub struct LoggingConfig {
    pub level: LogStructure,

    #[serde(default = "default_json")]
    pub json: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: LogStructure::Preset(LogLevel::Normal),
            json: false,
        }
    }
}

impl LoggingConfig {
    fn get_filter(&self) -> EnvFilter {
        let directives = match &self.level {
            LogStructure::Explicit(dirs) => dirs.clone(),
            &LogStructure::Preset(LogLevel::Normal) => DEFAULT_TRACKED_CRATES
                .iter()
                .map(|crate_name| format!("{}={}", crate_name, NORMAL_CRATE_LEVEL))
                .collect::<Vec<_>>(),
            LogStructure::Preset(LogLevel::Detailed) => DEFAULT_TRACKED_CRATES
                .iter()
                .map(|crate_name| format!("{}={}", crate_name, DETAILED_CRATE_LEVEL))
                .collect::<Vec<_>>(),
        };

        let joined_directives = directives.join(",");

        EnvFilter::new(joined_directives)
    }
}

fn get_logging_config(path: &Option<PathBuf>) -> Result<LoggingConfig> {
    let mut builder = Config::builder();
    builder = builder
        .set_default("level", "normal")?
        .set_default("json", false)?
        .add_source(config::Environment::with_prefix("MENMOS_LOG"));

    if let Some(path) = path {
        builder = builder.add_source(config::File::from(path.as_ref()))
    }

    let config: LoggingConfig = builder.build()?.try_deserialize()?;

    Ok(config)
}

pub fn init_logger(log_cfg_path: &Option<PathBuf>) -> Result<()> {
    let cfg = get_logging_config(log_cfg_path)?;

    let env_filter = cfg.get_filter();

    if cfg.json {
        FmtSubscriber::builder()
            .with_env_filter(env_filter)
            .json()
            .finish()
            .init();
    } else {
        FmtSubscriber::builder()
            .with_env_filter(env_filter)
            .finish()
            .init();
    }

    Ok(())
}
