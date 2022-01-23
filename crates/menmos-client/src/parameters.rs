use std::time;

pub struct Parameters {
    pub host: String,
    pub username: String,
    pub password: String,

    pub pool_idle_timeout: time::Duration,
    pub request_timeout: time::Duration,

    pub max_retry_count: usize,
    pub retry_interval: time::Duration,

    pub metadata_detection: bool,
}
