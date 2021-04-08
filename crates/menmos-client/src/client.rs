use std::path::{Path, PathBuf};
use std::time::Duration;

use apikit::payload::{ErrorResponse, MessageResponse};

use bytes::Bytes;

use futures::{Stream, TryStreamExt};

use header::HeaderName;
use interface::{BlobMeta, MetadataList, Query, QueryResponse};

use hyper::{header, StatusCode};

use mpart_async::client::MultipartRequest;

use protocol::{
    directory::{
        auth::{LoginRequest, LoginResponse, RegisterRequest},
        blobmeta::{GetMetaResponse, ListMetadataRequest},
        storage::ListStorageNodesResponse,
    },
    storage::PutResponse,
};
use reqwest::{Client as ReqwestClient, Request};

use reqwest::Body;

use serde::de::DeserializeOwned;
use snafu::{ensure, ResultExt, Snafu};

use crate::{
    metadata_detector::{MetadataDetector, MetadataDetectorError},
    parameters::HostConfig,
    profile::ProfileError,
    ClientBuilder, Config, Meta, Parameters,
};

#[derive(Debug, Snafu)]
pub enum ClientError {
    #[snafu(display("failed to build reqwest client: {}", source))]
    ClientBuildError { source: reqwest::Error },

    #[snafu(display("failed to fetch response body: {}", source))]
    FetchBodyError { source: reqwest::Error },

    #[snafu(display("file [{:?}] does not exist", path))]
    FileDoesNotExist { path: PathBuf },

    #[snafu(display("failed to serialize metadata [{:?}]: {}", meta, source))]
    MetaSerializationError {
        source: serde_json::Error,
        meta: Meta,
    },

    #[snafu(display("the redirect limit of {} was exceeded", limit))]
    RedirectLimitExceeded { limit: u32 },

    #[snafu(display("failed to build request: {}", source))]
    RequestBuildError { source: reqwest::Error },

    #[snafu(display("failed to execute request: {}", source))]
    RequestExecutionError { source: reqwest::Error },

    #[snafu(display("failed to deserialize response: {}", source))]
    ResponseDeserializationError { source: serde_json::Error },

    #[snafu(display("server returned an error: {}", message))]
    ServerReturnedError { message: String },

    #[snafu(display("failed to load configuration: {}", source))]
    ConfigLoadError { source: ProfileError },

    #[snafu(display("missing profile '{}'", name))]
    MissingProfile { name: String },

    #[snafu(display("did not get a redirect when expected"))]
    MissingRedirect,

    #[snafu(display("too many retries"))]
    TooManyRetries,

    #[snafu(display("unknown error"))]
    UnknownError,

    #[snafu(display("failed to instantiate the metadata detector: {}", source))]
    MetadataDetectorErrorInstantiationError { source: MetadataDetectorError },
}

fn encode_metadata(meta: Meta) -> Result<String> {
    let serialized_meta = serde_json::to_vec(&meta).context(MetaSerializationError { meta })?;
    Ok(base64::encode(&serialized_meta))
}

async fn extract_body<T: DeserializeOwned>(response: reqwest::Response) -> Result<T> {
    let body_bytes = response.bytes().await.context(FetchBodyError)?;
    serde_json::from_slice(body_bytes.as_ref()).context(ResponseDeserializationError)
}

async fn extract_error(response: reqwest::Response) -> ClientError {
    match extract_body::<ErrorResponse>(response).await {
        Ok(e) => ClientError::ServerReturnedError { message: e.error },
        Err(_) => ClientError::UnknownError,
    }
}

async fn extract<T: DeserializeOwned>(response: reqwest::Response) -> Result<T> {
    let status = response.status();
    if status.is_success() {
        extract_body(response).await
    } else {
        Err(extract_error(response).await)
    }
}

/// The client, used for interacting witn a Menmos cluster.
#[derive(Clone)]
pub struct Client {
    client: ReqwestClient,
    host: String,
    max_retry_count: usize,
    retry_interval: Duration,
    token: String,
    metadata_detector: Option<MetadataDetector>,
}

type Result<T> = std::result::Result<T, ClientError>;

impl Client {
    /// Create a new client from explicit credentials with default settings.
    pub async fn new<S: Into<String>, U: Into<String>, P: Into<String>>(
        directory_host: S,
        username: U,
        admin_password: P,
    ) -> Result<Self> {
        Client::new_with_params(Parameters {
            host_config: HostConfig::Host {
                host: directory_host.into(),
                username: username.into(),
                admin_password: admin_password.into(),
            },
            pool_idle_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(60),
            max_retry_count: 20,
            retry_interval: Duration::from_millis(100),
            metadata_detection: false,
        })
        .await
    }

    /// Create a new client from a configured profile with default settings.
    ///
    /// The profiles are read from
    /// * `$XDG_CONFIG_HOME/menmos/client.toml` on Linux
    /// * `~/Library/Application Support/menmos/client.toml` on MacOS
    /// * `%APPDATA%\menmos\client.toml` on Windows
    pub async fn new_with_profile<S: Into<String>>(profile: S) -> Result<Self> {
        Self::new_with_params(Parameters {
            host_config: HostConfig::Profile {
                profile: profile.into(),
            },
            pool_idle_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(60),
            max_retry_count: 20,
            retry_interval: Duration::from_millis(100),
            metadata_detection: false,
        })
        .await
    }

    /// Get a client builder to get better control on how the client is configured.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    pub(crate) async fn new_with_params(params: Parameters) -> Result<Self> {
        let client = ReqwestClient::builder()
            .pool_idle_timeout(params.pool_idle_timeout)
            .timeout(params.request_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context(ClientBuildError)?;

        let (host, username, admin_password) = match params.host_config {
            HostConfig::Host {
                host,
                username,
                admin_password,
            } => (host, username, admin_password),
            HostConfig::Profile { profile } => {
                let config = Config::load().context(ConfigLoadError)?;
                let profile = config
                    .profiles
                    .get(&profile)
                    .ok_or(ClientError::MissingProfile { name: profile })?;
                (
                    profile.host.clone(),
                    profile.username.clone(),
                    profile.password.clone(),
                )
            }
        };

        let token = Client::login(&client, &host, &username, &admin_password).await?;

        let metadata_detector = if params.metadata_detection {
            Some(MetadataDetector::new().context(MetadataDetectorErrorInstantiationError)?)
        } else {
            None
        };

        Ok(Self {
            host,
            client,
            max_retry_count: params.max_retry_count,
            retry_interval: params.retry_interval,
            token,
            metadata_detector,
        })
    }

    async fn execute<R: Fn() -> Result<Request>>(&self, req_fn: R) -> Result<reqwest::Response> {
        let mut attempt_count = 0;
        loop {
            match self
                .client
                .execute(req_fn()?)
                .await
                .context(RequestExecutionError)
            {
                Ok(r) => return Ok(r),
                Err(e) => {
                    log::debug!(
                        "request failed: {} - retrying in {}ms",
                        e,
                        self.retry_interval.as_millis()
                    );
                    attempt_count += 1;
                    if attempt_count >= self.max_retry_count {
                        return Err(e);
                    }

                    tokio::time::sleep(self.retry_interval).await;
                }
            };
        }
    }

    fn prepare_push_request<P: AsRef<Path>>(
        &self,
        url: &str,
        path: P,
        encoded_meta: &str,
    ) -> Result<reqwest::Request> {
        if path.as_ref().is_file() {
            let mut mpart = MultipartRequest::default();
            mpart.add_file("src", path.as_ref());

            self.client
                .post(url)
                .bearer_auth(&self.token)
                .header(
                    header::CONTENT_TYPE,
                    format!("multipart/form-data; boundary={}", mpart.get_boundary()),
                )
                .header(header::HeaderName::from_static("x-blob-meta"), encoded_meta)
                .body(Body::wrap_stream(mpart))
                .build()
                .context(RequestBuildError)
        } else {
            self.client
                .post(url)
                .bearer_auth(&self.token)
                .header(header::HeaderName::from_static("x-blob-meta"), encoded_meta)
                .build()
                .context(RequestBuildError)
        }
    }

    async fn request_with_redirect(&self, request: Request) -> Result<String> {
        let response = self
            .client
            .execute(request)
            .await
            .context(RequestExecutionError)?;

        ensure!(
            response.status() == StatusCode::TEMPORARY_REDIRECT,
            MissingRedirect
        );

        let new_location = response
            .headers()
            .get(header::LOCATION)
            .ok_or(ClientError::MissingRedirect)?;

        let new_url = String::from_utf8_lossy(new_location.as_bytes());
        log::debug!("redirect to {}", new_url);
        return Ok(new_url.to_string());
    }

    async fn login(
        client: &ReqwestClient,
        host: &str,
        username: &str,
        password: &str,
    ) -> Result<String> {
        let url = format!("{}/auth/login", host);

        let response = client
            .post(&url)
            .json(&LoginRequest {
                username: username.to_string(),
                password: password.to_string(),
            })
            .send()
            .await
            .context(RequestExecutionError)?;

        let resp: LoginResponse = extract(response).await?;

        Ok(resp.token)
    }

    pub async fn register(&self, username: &str, password: &str) -> Result<String> {
        let url = format!("{}/auth/register", self.host);

        let response = self
            .client
            .post(&url)
            .bearer_auth(&self.token)
            .json(&RegisterRequest {
                username: username.to_string(),
                password: password.to_string(),
            })
            .send()
            .await
            .context(RequestExecutionError)?;

        let resp: LoginResponse = extract(response).await?;
        Ok(resp.token)
    }

    /// Create an empty file on the cluster with the provided meta.
    ///
    /// Returns the created file's ID.
    pub async fn create_empty(&self, meta: Meta) -> Result<String> {
        let url = format!("{}/blob", self.host);
        let meta_b64 = encode_metadata(meta)?;

        let redirect_req = self
            .client
            .post(&url)
            .bearer_auth(&self.token)
            .header(HeaderName::from_static("x-blob-meta"), meta_b64.clone())
            .build()
            .context(RequestBuildError)?;

        let redirect_location = self.request_with_redirect(redirect_req).await?;

        let response = self
            .execute(|| {
                self.client
                    .post(&redirect_location)
                    .bearer_auth(&self.token)
                    .header(HeaderName::from_static("x-blob-meta"), &meta_b64)
                    .build()
                    .context(RequestBuildError)
            })
            .await?;

        let put_response: PutResponse = extract(response).await?;
        Ok(put_response.id)
    }

    async fn push_internal<P: AsRef<Path>>(
        &self,
        path: P,
        mut meta: Meta,
        base_url: String,
    ) -> Result<String> {
        ensure!(
            path.as_ref().exists(),
            FileDoesNotExist {
                path: PathBuf::from(path.as_ref())
            }
        );

        if let Some(metadata_detector) = self.metadata_detector.as_ref() {
            metadata_detector
                .populate(&path, &mut meta)
                .context(MetadataDetectorErrorInstantiationError)?;
        }

        let mut url = base_url;
        let meta_b64 = encode_metadata(meta)?;

        let initial_redirect_request = self
            .client
            .post(&url)
            .bearer_auth(&self.token)
            .header(
                header::HeaderName::from_static("x-blob-meta"),
                meta_b64.clone(),
            )
            .build()
            .context(RequestBuildError)?;

        url = self.request_with_redirect(initial_redirect_request).await?;

        let response = self
            .execute(|| self.prepare_push_request(&url, path.as_ref(), &meta_b64))
            .await?;

        let put_response: PutResponse = extract(response).await?;
        Ok(put_response.id)
    }

    /// Send a health check request to the cluster.
    ///
    /// Returns the cluster health status as a string.
    pub async fn health(&self) -> Result<String> {
        let url = format!("{}/health", self.host);

        let response = self
            .execute(|| self.client.get(&url).build().context(RequestBuildError))
            .await?;

        let status = response.status();

        if status.is_success() {
            let msg: MessageResponse = extract_body(response).await?;
            Ok(msg.message)
        } else {
            Err(extract_error(response).await)
        }
    }

    /// List all storage nodes currently authenticated with the cluster.
    pub async fn list_storage_nodes(&self) -> Result<ListStorageNodesResponse> {
        let url = format!("{}/node/storage", self.host);

        let response = self
            .execute(|| {
                self.client
                    .get(&url)
                    .bearer_auth(&self.token)
                    .build()
                    .context(RequestBuildError)
            })
            .await?;

        extract_body(response).await
    }

    /// Pushes a file with the specified meta to the cluster.
    ///
    /// Returns the ID of the created file.
    pub async fn push<P: AsRef<Path>>(&self, path: P, meta: Meta) -> Result<String> {
        self.push_internal(path, meta, format!("{}/blob", self.host))
            .await
    }

    /// Update a blob's content.
    ///
    /// Returns the ID of the updated file. Should always be equal to `blob_id`.
    pub async fn update_blob<P: AsRef<Path>>(
        &self,
        blob_id: &str,
        path: P,
        meta: Meta,
    ) -> Result<String> {
        self.push_internal(path, meta, format!("{}/blob/{}", self.host, blob_id))
            .await
    }

    /// Lists all metadata values in the cluster.
    ///
    /// `tags` is an optional whitelist of tags to compute. When absent, all tags are included.
    /// `meta_keys` is an optional whitelist of keys to compute. When absent, all key/value pairs are included (this can be very expensive).
    pub async fn list_meta(
        &self,
        tags: Option<Vec<String>>,
        meta_keys: Option<Vec<String>>,
    ) -> Result<MetadataList> {
        let url = format!("{}/metadata", &self.host);

        let response = self
            .client
            .get(&url)
            .bearer_auth(&self.token)
            .json(&ListMetadataRequest { tags, meta_keys })
            .send()
            .await
            .context(RequestExecutionError)?;

        extract(response).await
    }

    /// Update a blob's metadata without touching the contents of the file.
    pub async fn update_meta(&self, blob_id: &str, meta: Meta) -> Result<()> {
        let url = format!("{}/blob/{}/metadata", self.host, blob_id);

        let request = self
            .client
            .put(&url)
            .bearer_auth(&self.token)
            .json(&meta)
            .build()
            .context(RequestBuildError)?;

        let redirect_location = self.request_with_redirect(request).await?;

        let response = self
            .execute(|| {
                self.client
                    .put(&redirect_location)
                    .bearer_auth(&self.token)
                    .json(&meta)
                    .build()
                    .context(RequestBuildError)
            })
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(extract_error(response).await)
        }
    }

    /// Force synchronization of a blob to its backing storage.
    ///
    /// This is an advanced feature, and should only be called by people who know what they are doing.
    pub async fn fsync(&self, blob_id: &str) -> Result<()> {
        let url = format!("{}/blob/{}/fsync", self.host, blob_id);

        let request = self
            .client
            .post(&url)
            .bearer_auth(&self.token)
            .build()
            .context(RequestBuildError)?;

        let redirect_location = self.request_with_redirect(request).await?;

        let response = self
            .execute(|| {
                self.client
                    .post(&redirect_location)
                    .bearer_auth(&self.token)
                    .build()
                    .context(RequestBuildError)
            })
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(extract_error(response).await)
        }
    }

    /// Write a byte buffer to a specified offset in a blob.
    pub async fn write(&self, blob_id: &str, offset: u64, buffer: Bytes) -> Result<()> {
        let url = format!("{}/blob/{}", self.host, blob_id);

        let request = self
            .client
            .put(&url)
            .bearer_auth(&self.token)
            .header(
                header::RANGE,
                &format!("bytes={}-{}", offset, offset + (buffer.len() - 1) as u64),
            )
            .build()
            .context(RequestBuildError)?;

        let redirect_location = self.request_with_redirect(request).await?;

        let response = self
            .execute(|| {
                self.client
                    .put(&redirect_location)
                    .bearer_auth(&self.token)
                    .header(
                        header::RANGE,
                        &format!("bytes={}-{}", offset, offset + (buffer.len() - 1) as u64),
                    )
                    .body(buffer.clone())
                    .build()
                    .context(RequestBuildError)
            })
            .await?;

        let status = response.status();
        if status.is_success() {
            // Our upload got through.
            // Deserialize the body to get the content ID.
            return Ok(());
        } else {
            // An error occurred.
            return Err(extract_error(response).await);
        }
    }

    /// Get a blob's metadata.
    pub async fn get_meta(&self, blob_id: &str) -> Result<Option<BlobMeta>> {
        let url = format!("{}/blob/{}/metadata", self.host, blob_id);

        let response = self
            .execute(|| {
                self.client
                    .get(&url)
                    .bearer_auth(&self.token)
                    .build()
                    .context(RequestBuildError)
            })
            .await?;

        let resp: GetMetaResponse = extract(response).await?;
        Ok(resp.meta)
    }

    /// Get a blob's body as a stream of bytes.
    pub async fn get_file(&self, blob_id: &str) -> Result<impl Stream<Item = Result<Bytes>>> {
        let url = format!("{}/blob/{}", self.host, blob_id);

        let redirect_request = self
            .client
            .get(&url)
            .bearer_auth(&self.token)
            .build()
            .context(RequestBuildError)?;

        let redirect_location = self.request_with_redirect(redirect_request).await?;

        let response = self
            .execute(|| {
                self.client
                    .get(&redirect_location)
                    .bearer_auth(&self.token)
                    .build()
                    .context(RequestBuildError)
            })
            .await?;

        if response.status().is_success() {
            Ok(response
                .bytes_stream()
                .map_err(|_| ClientError::UnknownError))
        } else {
            Err(extract_error(response).await)
        }
    }

    // TODO: This API might be improved by using a bytes buffer instead of a raw vec.
    // TODO: Use a rust range instead of a tuple
    // TODO: Return a stream of Bytes buffers.
    // Note: range is end-inclusive here. TODO: Clarify when ranges are inclusive vs. exclusive.
    /// Read a subset of a blob.
    ///
    /// The `range` argument is end-inclusive.
    pub async fn read_range(&self, blob_id: &str, range: (u64, u64)) -> Result<Vec<u8>> {
        let url = format!("{}/blob/{}", self.host, blob_id);

        let request = self
            .client
            .get(&url)
            .bearer_auth(&self.token)
            .header(header::RANGE, &format!("bytes={}-{}", range.0, range.1))
            .build()
            .context(RequestBuildError)?;

        let redirect_location = self.request_with_redirect(request).await?;

        let response = self
            .execute(|| {
                self.client
                    .get(&redirect_location)
                    .header(header::RANGE, &format!("bytes={}-{}", range.0, range.1))
                    .bearer_auth(&self.token)
                    .build()
                    .context(RequestBuildError)
            })
            .await?;

        let status = response.status();
        if status.is_success() {
            let resp_bytes = response.bytes().await.context(FetchBodyError)?;
            Ok(resp_bytes.to_vec())
        } else {
            Err(extract_error(response).await)
        }
    }

    /// Send a query to the cluster.
    pub async fn query(&self, query: Query) -> Result<QueryResponse> {
        let url = format!("{}/query", self.host);

        let response = self
            .execute(|| {
                self.client
                    .post(&url)
                    .bearer_auth(&self.token)
                    .json(&query)
                    .build()
                    .context(RequestBuildError)
            })
            .await?;
        extract(response).await
    }

    /// Delete a blob from the cluster.
    pub async fn delete(&self, blob_id: String) -> Result<()> {
        let url = format!("{}/blob/{}", self.host, blob_id);

        let request = self
            .client
            .delete(&url)
            .bearer_auth(&self.token)
            .build()
            .context(RequestBuildError)?;

        let redirect_location = self.request_with_redirect(request).await?;

        let response = self
            .execute(|| {
                self.client
                    .delete(&redirect_location)
                    .bearer_auth(&self.token)
                    .build()
                    .context(RequestBuildError)
            })
            .await?;

        let status = response.status();
        if status.is_success() {
            // Our delete got through.
            return Ok(());
        } else {
            // An error occurred.
            return Err(extract_error(response).await);
        }
    }
}
