use std::time;

pub struct Parameters {
    pub host: String,
    pub admin_password: String,

    pub pool_idle_timeout: time::Duration,
    pub request_timeout: time::Duration,
}
