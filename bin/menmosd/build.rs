use std::{
    path::PathBuf,
    process::{exit, Command},
};

use anyhow::{bail, ensure, Result};

// TODO: Allow override by env.
// TODO: Checkout at same version tag as menmosd.
const WEB_REPO_PATH: &str = "https://github.com/menmos/menmos-web.git";

const LOCAL_PATH: &str = "menmos-web";

enum Target<S>
where
    S: AsRef<str>,
{
    Branch(S),
    Tag(S),
}

impl<S: AsRef<str>> AsRef<str> for Target<S> {
    fn as_ref(&self) -> &str {
        match &self {
            Target::Tag(s) | Target::Branch(s) => s.as_ref(),
        }
    }
}

fn run(args: &[&str]) -> Result<()> {
    ensure!(args.len() > 0);
    let mut handle = Command::new(args[0]).args(&args[1..]).spawn()?;
    let result = handle.wait()?;
    ensure!(result.success(), "command failed: {}", result.to_string());
    Ok(())
}

fn ensure_clone<S: AsRef<str>>(target: Target<S>) -> Result<()> {
    // TODO: Detect if git exists.
    let tgt_path = PathBuf::from(LOCAL_PATH);
    if !tgt_path.exists() {
        run(&["git", "clone", WEB_REPO_PATH, LOCAL_PATH])?;
    }

    std::env::set_current_dir(&tgt_path)?;

    // Make sure repo is up to date.
    run(&["git", "fetch", "--prune"])?;
    run(&["git", "checkout", target.as_ref()])?;

    if let Target::Branch(b) = target {
        // Git pull makes no sense for a tag target.
        run(&["git", "pull", "origin", b.as_ref()])?;
    }

    Ok(())
}

fn npm_build() -> Result<()> {
    // Do the build
    run(&["npm", "install"])?;
    run(&["npm", "run", "build"])?;
    run(&["npm", "run", "export"])
}

fn parse_env_var(val: &str) -> Result<Target<String>> {
    match val {
        "latest" => {
            bail!("TODO: implement tag detection");
        }
        s if s.starts_with("tag=") => Ok(Target::Tag(
            s.strip_prefix("tag=").unwrap_or_default().to_string(),
        )),
        s if s.starts_with("branch=") => Ok(Target::Branch(
            s.strip_prefix("branch=").unwrap_or_default().to_string(),
        )),
        _ => bail!("unknown target: {}", val),
    }
}

fn apply(tgt_env: String) -> Result<()> {
    // TODO: Branch & version arguments. (default to latest tag, can specify either branch or tag in env var).
    let target = parse_env_var(&tgt_env)?;
    ensure_clone(target)?;
    npm_build()?;

    Ok(())
}

fn main() {
    println!("cargo:rerun-if-env-changed=MENMOS_WEBUI");
    println!("cargo:rerun-if-changed=build.rs");

    if let Ok(val) = std::env::var("MENMOS_WEBUI") {
        if let Err(e) = apply(val) {
            eprintln!("{}", e);
            exit(1);
        }
    }
}
