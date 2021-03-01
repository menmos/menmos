use std::net::{IpAddr, SocketAddr};

use serde::{Deserialize, Serialize};

const DEFAULT_CONCURRENT_REQUESTS: usize = 40;

fn default_concurrent_requests() -> usize {
    DEFAULT_CONCURRENT_REQUESTS
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub host_name: String,
    pub root_domain: String,
    pub public_ip: IpAddr,
    pub listen: SocketAddr,

    #[serde(default = "default_concurrent_requests")]
    pub nb_of_concurrent_requests: usize,
}
