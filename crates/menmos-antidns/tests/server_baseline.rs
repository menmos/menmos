use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use anyhow::{anyhow, Result};

use antidns::{Config, Server};
use trust_dns_resolver::{
    config::{NameServerConfig, ResolverConfig},
    proto::rr::{RData, Record},
    TokioAsyncResolver,
};

async fn get_server() -> Result<(Server, TokioAsyncResolver)> {
    let socket_port =
        portpicker::pick_unused_port().ok_or_else(|| anyhow!("failed to pick port"))?;

    let cfg = Config {
        host_name: "dns.menmos.org".into(),
        root_domain: "menmos.org".into(),
        public_ip: IpAddr::from([127, 0, 0, 1]),
        listen: SocketAddr::new(IpAddr::from([0, 0, 0, 0]), socket_port),
        nb_of_concurrent_requests: 8,
    };

    let mut resolver_cfg = ResolverConfig::new();
    resolver_cfg.add_name_server(NameServerConfig {
        bind_addr: None,
        socket_addr: SocketAddr::new(IpAddr::from([127, 0, 0, 1]), socket_port),
        protocol: trust_dns_resolver::config::Protocol::Udp,
        tls_dns_name: None,
        trust_nx_responses: true,
    });

    let resolver = TokioAsyncResolver::tokio(resolver_cfg, Default::default())?;
    Ok((Server::start(cfg), resolver))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dns_server_lifecycle() -> Result<()> {
    let (server, _resolver) = get_server().await?;
    server.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dns_resolution_of_magic_domain_names() -> Result<()> {
    let (server, resolver) = get_server().await?;

    let result: Vec<Record> = resolver
        .lookup_ip("192-168-2-100.menmos.org")
        .await?
        .as_lookup()
        .record_iter()
        .cloned()
        .collect();

    assert_eq!(result.len(), 1);

    if let Some(RData::A(ip)) = result[0].data() {
        assert_eq!(ip, &Ipv4Addr::from([192, 168, 2, 100]));
    } else {
        return Err(anyhow!("missing A record"));
    }

    server.stop().await?;
    Ok(())
}
