use std::time;

use snafu::{ensure, ResultExt, Snafu};

use crate::client::ClientError;
use crate::{Client, Parameters};

#[derive(Debug, Snafu)]
pub enum BuildError {
    #[snafu(display("missing host"))]
    MissingHost,

    #[snafu(display("missing username"))]
    MissingUsername,

    #[snafu(display("missing password"))]
    MissingPassword,

    #[snafu(display("failed to build: {}", source))]
    Build { source: ClientError },
}

pub struct ClientBuilder {
    host: Option<String>,
    username: Option<String>,
    password: Option<String>,

    pool_idle_timeout: time::Duration,
    request_timeout: time::Duration,

    max_retry_count: usize,
    retry_interval: time::Duration,

    metadata_detection: bool,
}

impl ClientBuilder {
    pub fn with_host<S: Into<String>>(mut self, host: S) -> Self {
        self.host = Some(host.into());
        self
    }

    pub fn with_username<S: Into<String>>(mut self, username: S) -> Self {
        self.username = Some(username.into());
        self
    }

    pub fn with_password<S: Into<String>>(mut self, password: S) -> Self {
        self.password = Some(password.into());
        self
    }

    pub fn with_pool_idle_timeout<T: Into<time::Duration>>(mut self, timeout: T) -> Self {
        self.pool_idle_timeout = timeout.into();
        self
    }

    pub fn with_request_timeout<T: Into<time::Duration>>(mut self, timeout: T) -> Self {
        self.request_timeout = timeout.into();
        self
    }

    pub fn with_max_retry_count(mut self, count: usize) -> Self {
        self.max_retry_count = count;
        self
    }

    pub fn with_retry_interval<T: Into<time::Duration>>(mut self, interval: T) -> Self {
        self.retry_interval = interval.into();
        self
    }

    pub fn with_metadata_detection(mut self) -> Self {
        self.metadata_detection = true;
        self
    }

    pub async fn build(self) -> Result<Client, BuildError> {
        ensure!(self.host.is_some(), MissingHostSnafu);
        ensure!(self.password.is_some(), MissingPasswordSnafu);
        ensure!(self.username.is_some(), MissingUsernameSnafu);

        let params = Parameters {
            host: self.host.unwrap(),
            username: self.username.unwrap(),
            password: self.password.unwrap(),
            pool_idle_timeout: self.pool_idle_timeout,
            request_timeout: self.request_timeout,
            max_retry_count: self.max_retry_count,
            retry_interval: self.retry_interval,
        };

        Client::new_with_params(params).await.context(BuildSnafu)
    }
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            host: None,
            username: None,
            password: None,
            pool_idle_timeout: time::Duration::from_secs(5),
            request_timeout: time::Duration::from_secs(60),
            max_retry_count: 20,
            retry_interval: time::Duration::from_millis(100),
            metadata_detection: false,
        }
    }
}
