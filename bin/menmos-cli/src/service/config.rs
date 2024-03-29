use anyhow::{anyhow, Result};
use menmos::{Config, Profile};
use rood::cli::OutputManager;

pub fn load_or_create(cli: OutputManager) -> Result<Config> {
    // TODO: Ditch map_err once menmos-sdk stops using Whatever
    let mut cfg = Config::load().map_err(|e| anyhow!("{e}"))?;

    if cfg.profiles.is_empty() {
        cli.step("No config is present - Creating a default profile");
        let default_profile = make_default(cli.push())?;
        cfg.add("default", default_profile)
            .map_err(|e| anyhow!("{e}"))?;
        cli.success("Configuration complete");
    }

    Ok(cfg)
}

fn make_default(cli: OutputManager) -> Result<Profile> {
    let host = cli.prompt("Directory node host: ")?;
    let username = cli.prompt("Directory node username: ")?;
    let password = cli.prompt_password("Directory node password: ")?;
    Ok(Profile {
        host,
        username,
        password,
    })
}
