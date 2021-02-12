//! Tests basic functionality of menmosd, without a storage node attached.

use std::path::PathBuf;

use anyhow::Result;
use interface::QueryResponse;
use menmos_client::{Client, Query};
use menmosd::{
    config::{HTTPParameters, ServerSetting},
    Config, Directory, Index, Server,
};
use tempfile::TempDir;

async fn get_node<P: Into<PathBuf>>(db_path: P) -> Result<(Server<Directory<Index>>, Client)> {
    const MENMOSD_CONFIG: &str = include_str!("data/menmosd_http.toml");

    let mut cfg = Config::from_toml_string(MENMOSD_CONFIG)?;
    cfg.node.db_path = db_path.into();

    let port = portpicker::pick_unused_port().unwrap();
    cfg.server = ServerSetting::HTTP(HTTPParameters { port });

    let node = menmosd::make_node(&cfg)?;
    let client = Client::new(format!("http://localhost:{}", port), "password")?;
    Ok((Server::new(cfg, node).await?, client))
}

#[tokio::test]
async fn comes_up_and_stops() -> Result<()> {
    let db_dir = TempDir::new()?;
    let (server, client) = get_node(db_dir.path()).await?;

    // Make sure the server responds.
    client.health().await?;

    // Make sure it stops.
    server.stop().await?;

    Ok(())
}

#[tokio::test]
async fn queries_initially_return_empty() -> Result<()> {
    let db_dir = TempDir::new()?;
    let (server, client) = get_node(db_dir.path()).await?;

    let actual_response = client.query(Query::default()).await?;

    let expected_response = QueryResponse {
        count: 0,
        total: 0,
        hits: Vec::default(),
        facets: None,
    };

    assert_eq!(expected_response, actual_response);

    server.stop().await?;

    Ok(())
}
