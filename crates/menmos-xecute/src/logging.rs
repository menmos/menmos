use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;

use config::Config;

use serde::{Deserialize, Serialize};

use tracing_subscriber::prelude::*;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

use super::telemetry;

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
    "axum",
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

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum TracingConfig {
    /// Default to the local jaeger collector.
    None,
    Jaeger {
        host: Option<SocketAddr>,
    },
    OTLP,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct LoggingConfig {
    pub level: LogStructure,

    pub tracing: TracingConfig,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: LogStructure::Preset(LogLevel::Normal),
            tracing: TracingConfig::None,
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
        .set_default("tracing.type", "none")?
        .add_source(config::Environment::with_prefix("MENMOS_LOG"));

    if let Some(path) = path {
        builder = builder.add_source(config::File::from(path.as_ref()))
    }

    let config: LoggingConfig = builder.build()?.try_deserialize()?;

    Ok(config)
}

pub fn init_logger(name: &str, log_cfg_path: &Option<PathBuf>) -> Result<()> {
    let cfg = get_logging_config(log_cfg_path)?;

    // The env filter logs only
    let env_filter = cfg.get_filter();

    if let Some(tracer) = telemetry::init_tracer(name, &cfg.tracing)? {
        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        Registry::default()
            .with(env_filter)
            .with(telemetry)
            .with(tracing_subscriber::fmt::layer())
            .init();
    } else {
        Registry::default()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer())
            .init();
    }

    Ok(())
}
