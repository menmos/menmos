use std::time;

pub enum HostConfig {
    Host {
        host: String,
        username: String,
        admin_password: String,
    },
    Profile {
        profile: String,
    },
}

pub struct Parameters {
    pub host_config: HostConfig,

    pub pool_idle_timeout: time::Duration,
    pub request_timeout: time::Duration,

    pub max_retry_count: usize,
    pub retry_interval: time::Duration,

    pub metadata_detection: bool,
}
