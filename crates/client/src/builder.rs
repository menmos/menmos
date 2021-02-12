use std::time;

use snafu::{ensure, ResultExt, Snafu};

use crate::omniclient::ClientError;
use crate::{Client, Parameters};

#[derive(Debug, Snafu)]
pub enum BuildError {
    #[snafu(display("missing host"))]
    MissingHost,

    #[snafu(display("missing password"))]
    MissingPassword,

    #[snafu(display("failed to build: {}", source))]
    ClientBuildError { source: ClientError },
}

pub struct ClientBuilder {
    host: Option<String>,
    admin_password: Option<String>,

    pool_idle_timeout: time::Duration,
    request_timeout: time::Duration,
}

impl ClientBuilder {
    pub fn with_host<S: Into<String>>(mut self, host: S) -> Self {
        self.host = Some(host.into());
        self
    }

    pub fn with_password<S: Into<String>>(mut self, password: S) -> Self {
        self.admin_password = Some(password.into());
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

    pub fn build(self) -> Result<Client, BuildError> {
        ensure!(self.host.is_some(), MissingHost);
        ensure!(self.admin_password.is_some(), MissingPassword);

        let params = Parameters {
            host: self.host.unwrap(),
            admin_password: self.admin_password.unwrap(),
            pool_idle_timeout: self.pool_idle_timeout,
            request_timeout: self.request_timeout,
        };

        Client::new_with_params(params).context(ClientBuildError)
    }
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            host: None,
            admin_password: None,
            pool_idle_timeout: time::Duration::from_secs(5),
            request_timeout: time::Duration::from_secs(60),
        }
    }
}
