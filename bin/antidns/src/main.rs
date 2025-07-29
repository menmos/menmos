use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

use antidns::{Config, Server};

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    xecute::logging::init_logger("antidns", &None)?;

    let config = Config {
        host_name: "hello.world".into(),
        root_domain: "hello.world".into(),
        public_ip: IpAddr::from_str("127.0.0.1").unwrap(),
        listen: SocketAddr::from_str("127.0.0.1:8053").unwrap(),
        nb_of_concurrent_requests: 100,
    };

    let server = Server::start(config);

    tokio::signal::ctrl_c().await?;

    server.stop().await?;

    Ok(())
}
