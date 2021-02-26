use std::path::PathBuf;

use anyhow::Result;

use log::LevelFilter;

use log4rs::append::console::ConsoleAppender;
use log4rs::config::{Appender, Config, Root};

fn get_default_config() -> Result<Config> {
    let stdout = ConsoleAppender::builder().build();
    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(Root::builder().appender("stdout").build(LevelFilter::Info))?;
    Ok(config)
}

pub fn init_logger(log_cfg_path: &Option<PathBuf>) -> Result<()> {
    match &log_cfg_path {
        Some(log_path) => {
            log4rs::init_file(log_path, Default::default())?;
        }
        None => {
            let config = get_default_config()?;
            log4rs::init_config(config)?;
        }
    };

    Ok(())
}
