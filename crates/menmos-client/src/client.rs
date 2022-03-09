use std::path::{Path, PathBuf};
use std::time::Duration;

use apikit::payload::{ErrorResponse, MessageResponse};

use bytes::Bytes;

use futures::{Stream, TryStreamExt};

use header::HeaderName;

use interface::{BlobMeta, MetadataList, Query, QueryResponse, RoutingConfig};

use hyper::{header, StatusCode};

use protocol::directory::{auth::*, blobmeta::*, routing::*, storage::*};
use protocol::storage::PutResponse;

use reqwest::{Client as ReqwestClient, Request};

use reqwest::Body;

use serde::de::DeserializeOwned;

use snafu::prelude::*;
use tokio_util::codec::{BytesCodec, FramedRead};

use crate::{ClientBuilder, Meta, Parameters};

#[derive(Debug, Snafu)]
pub enum ClientError {
    #[snafu(display("failed to build reqwest client: {}", source))]
    ClientBuildError { source: reqwest::Error },

    #[snafu(display("failed to fetch response body: {}", source))]
    FetchBodyError { source: reqwest::Error },

    #[snafu(display("file [{:?}] does not exist", path))]
    FileDoesNotExist { path: PathBuf },

    #[snafu(display("failed to load the metadata for '{:?}': {}", path, source))]
    FileMetadataError {
        source: std::io::Error,
        path: PathBuf,
    },

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

    #[snafu(display("did not get a redirect when expected"))]
    MissingRedirect,

    #[snafu(display("did not receive a request id when expected"))]
    MissingRequestId,

    #[snafu(display("too many retries"))]
    TooManyRetries,

    #[snafu(display("{}", message))]
    UnknownError { message: String },
}

fn encode_metadata(meta: Meta) -> Result<String> {
    let serialized_meta = serde_json::to_vec(&meta).context(MetaSerializationSnafu { meta })?;
    Ok(base64::encode(&serialized_meta))
}

async fn extract_body<T: DeserializeOwned>(response: reqwest::Response) -> Result<T> {
    let body_bytes = response.bytes().await.context(FetchBodySnafu)?;
    tracing::debug!("body: {}", String::from_utf8_lossy(&body_bytes));
    serde_json::from_slice(body_bytes.as_ref()).context(ResponseDeserializationSnafu)
}

async fn extract_error(response: reqwest::Response) -> ClientError {
    match extract_body::<ErrorResponse>(response).await {
        Ok(e) => ClientError::ServerReturnedError { message: e.error },
        Err(e) => ClientError::UnknownError {
            message: e.to_string(),
        },
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

struct RedirectResponse {
    pub location: String,
    pub request_id: String,
}

/// The client, used for interacting witn a Menmos cluster.
#[derive(Clone)]
pub struct Client {
    client: ReqwestClient,
    host: String,
    token: String,
}

type Result<T> = std::result::Result<T, ClientError>;

impl Client {
    /// Create a new client from explicit credentials with default settings.
    pub async fn new<S: Into<String>, U: Into<String>, P: Into<String>>(
        directory_host: S,
        username: U,
        password: P,
    ) -> Result<Self> {
        Client::new_with_params(Parameters {
            host: directory_host.into(),
            username: username.into(),
            password: password.into(),
            pool_idle_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(60),
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
            .context(ClientBuildSnafu)?;

        let token =
            Client::login(&client, &params.host, &params.username, &params.password).await?;

        Ok(Self {
            host: params.host,
            client,
            token,
        })
    }

    async fn prepare_push_request<P: AsRef<Path>>(
        &self,
        url: &str,
        request_id: &str,
        path: P,
        encoded_meta: &str,
        file_length: u64,
    ) -> Result<reqwest::Request> {
        let mut request_builder = self
            .client
            .post(url)
            .bearer_auth(&self.token)
            .header(header::HeaderName::from_static("x-blob-meta"), encoded_meta)
            .header(header::HeaderName::from_static("x-request-id"), request_id);

        if path.as_ref().is_file() {
            let file = tokio::fs::File::open(path.as_ref()).await.unwrap();
            let stream = FramedRead::new(file, BytesCodec::new());
            request_builder = request_builder
                .body(Body::wrap_stream(stream))
                .header(HeaderName::from_static("x-blob-size"), file_length);
        } else {
            request_builder = request_builder.header(HeaderName::from_static("x-blob-size"), 0_u64);
        }

        request_builder.build().context(RequestBuildSnafu)
    }

    async fn request_with_redirect(&self, request: Request) -> Result<RedirectResponse> {
        let response = self
            .client
            .execute(request)
            .await
            .context(RequestExecutionSnafu)?;

        ensure!(
            response.status() == StatusCode::TEMPORARY_REDIRECT,
            MissingRedirectSnafu
        );

        let new_location = response
            .headers()
            .get(header::LOCATION)
            .ok_or(ClientError::MissingRedirect)?;

        let request_id = response
            .headers()
            .get("x-request-id")
            .ok_or(ClientError::MissingRequestId)?;

        let new_url = String::from_utf8_lossy(new_location.as_bytes());
        tracing::debug!("redirect to {}", new_url);

        Ok(RedirectResponse {
            location: new_url.to_string(),
            request_id: String::from_utf8(request_id.as_bytes().to_vec()).unwrap(), // We know request id is ASCII, so it is also unicode.
        })
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
            .context(RequestExecutionSnafu)?;

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
            .context(RequestExecutionSnafu)?;

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
            .header(HeaderName::from_static("x-blob-size"), 0_u64)
            .build()
            .context(RequestBuildSnafu)?;

        let RedirectResponse {
            location,
            request_id,
        } = self.request_with_redirect(redirect_req).await?;

        let response = self
            .client
            .post(&location)
            .bearer_auth(&self.token)
            .header(HeaderName::from_static("x-blob-meta"), &meta_b64)
            .header(HeaderName::from_static("x-blob-size"), 0_u64)
            .header(HeaderName::from_static("x-request-id"), &request_id)
            .send()
            .await
            .context(RequestExecutionSnafu)?;

        let put_response: PutResponse = extract(response).await?;
        Ok(put_response.id)
    }

    async fn push_internal<P: AsRef<Path>>(
        &self,
        path: P,
        meta: Meta,
        base_url: String,
    ) -> Result<String> {
        ensure!(
            path.as_ref().exists(),
            FileDoesNotExistSnafu {
                path: PathBuf::from(path.as_ref())
            }
        );

        let url = base_url;
        let meta_b64 = encode_metadata(meta)?;

        let file_length = path
            .as_ref()
            .metadata()
            .context(FileMetadataSnafu {
                path: PathBuf::from(path.as_ref()),
            })?
            .len();

        let initial_redirect_request = self
            .client
            .post(&url)
            .bearer_auth(&self.token)
            .header(
                header::HeaderName::from_static("x-blob-meta"),
                meta_b64.clone(),
            )
            .header(HeaderName::from_static("x-blob-size"), file_length)
            .build()
            .context(RequestBuildSnafu)?;

        let RedirectResponse {
            location,
            request_id,
        } = self.request_with_redirect(initial_redirect_request).await?;

        let request = self
            .prepare_push_request(
                &location,
                &request_id,
                path.as_ref(),
                &meta_b64,
                file_length,
            )
            .await?;

        let response = self
            .client
            .execute(request)
            .await
            .context(RequestExecutionSnafu)?;

        let put_response: PutResponse = extract(response).await?;
        Ok(put_response.id)
    }

    /// Send a health check request to the cluster.
    ///
    /// Returns the cluster health status as a string.
    pub async fn health(&self) -> Result<String> {
        let url = format!("{}/health", self.host);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context(RequestExecutionSnafu)?;

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
            .client
            .get(&url)
            .bearer_auth(&self.token)
            .send()
            .await
            .context(RequestExecutionSnafu)?;

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
            .json(&ListMetadataRequest {
                tags,
                fields: meta_keys,
            })
            .send()
            .await
            .context(RequestExecutionSnafu)?;

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
            .context(RequestBuildSnafu)?;

        let RedirectResponse {
            location,
            request_id,
        } = self.request_with_redirect(request).await?;

        let response = self
            .client
            .put(&location)
            .bearer_auth(&self.token)
            .header(HeaderName::from_static("x-request-id"), request_id)
            .json(&meta)
            .send()
            .await
            .context(RequestExecutionSnafu)?;

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
            .context(RequestBuildSnafu)?;

        let RedirectResponse {
            location,
            request_id,
        } = self.request_with_redirect(request).await?;

        let response = self
            .client
            .post(&location)
            .bearer_auth(&self.token)
            .header(HeaderName::from_static("x-request-id"), request_id)
            .send()
            .await
            .context(RequestBuildSnafu)?;

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
            .context(RequestBuildSnafu)?;

        let RedirectResponse {
            location,
            request_id,
        } = self.request_with_redirect(request).await?;

        let response = self
            .client
            .put(&location)
            .bearer_auth(&self.token)
            .header(
                header::RANGE,
                &format!("bytes={}-{}", offset, offset + (buffer.len() - 1) as u64),
            )
            .header(HeaderName::from_static("x-request-id"), request_id)
            .body(buffer.clone())
            .send()
            .await
            .context(RequestExecutionSnafu)?;

        let status = response.status();
        if status.is_success() {
            // Our upload got through.
            // Deserialize the body to get the content ID.
            Ok(())
        } else {
            // An error occurred.
            Err(extract_error(response).await)
        }
    }

    /// Get a blob's metadata.
    pub async fn get_meta(&self, blob_id: &str) -> Result<Option<BlobMeta>> {
        let url = format!("{}/blob/{}/metadata", self.host, blob_id);

        let response = self
            .client
            .get(&url)
            .bearer_auth(&self.token)
            .send()
            .await
            .context(RequestExecutionSnafu)?;

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
            .context(RequestBuildSnafu)?;

        let RedirectResponse {
            location,
            request_id,
        } = self.request_with_redirect(redirect_request).await?;

        let response = self
            .client
            .get(&location)
            .bearer_auth(&self.token)
            .header(HeaderName::from_static("x-request-id"), request_id)
            .send()
            .await
            .context(RequestExecutionSnafu)?;

        if response.status().is_success() {
            Ok(response
                .bytes_stream()
                .map_err(|e| ClientError::UnknownError {
                    message: e.to_string(),
                }))
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
            .context(RequestBuildSnafu)?;

        let RedirectResponse {
            location,
            request_id,
        } = self.request_with_redirect(request).await?;

        let response = self
            .client
            .get(&location)
            .header(header::RANGE, &format!("bytes={}-{}", range.0, range.1))
            .header(HeaderName::from_static("x-request-id"), request_id)
            .bearer_auth(&self.token)
            .send()
            .await
            .context(RequestExecutionSnafu)?;

        let status = response.status();
        if status.is_success() {
            let resp_bytes = response.bytes().await.context(FetchBodySnafu)?;
            Ok(resp_bytes.to_vec())
        } else {
            Err(extract_error(response).await)
        }
    }

    /// Send a query to the cluster.
    pub async fn query(&self, query: Query) -> Result<QueryResponse> {
        let url = format!("{}/query", self.host);

        let response = self
            .client
            .post(&url)
            .bearer_auth(&self.token)
            .json(&query)
            .send()
            .await
            .context(RequestExecutionSnafu)?;
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
            .context(RequestBuildSnafu)?;

        let RedirectResponse {
            location,
            request_id,
        } = self.request_with_redirect(request).await?;

        let response = self
            .client
            .delete(&location)
            .bearer_auth(&self.token)
            .header(HeaderName::from_static("x-request-id"), request_id)
            .send()
            .await
            .context(RequestExecutionSnafu)?;

        let status = response.status();
        if status.is_success() {
            // Our delete got through.
            Ok(())
        } else {
            // An error occurred.
            Err(extract_error(response).await)
        }
    }

    pub async fn get_routing_config(&self) -> Result<Option<RoutingConfig>> {
        let url = format!("{}/routing", self.host);

        let response = self
            .client
            .get(&url)
            .bearer_auth(&self.token)
            .send()
            .await
            .context(RequestExecutionSnafu)?;

        let response: GetRoutingConfigResponse = extract(response).await?;

        Ok(response.routing_config)
    }

    pub async fn set_routing_config(&self, routing_config: &RoutingConfig) -> Result<()> {
        let url = format!("{}/routing", self.host);

        let response = self
            .client
            .put(&url)
            .bearer_auth(&self.token)
            .json(&SetRoutingConfigRequest {
                routing_config: routing_config.clone(),
            })
            .send()
            .await
            .context(RequestExecutionSnafu)?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(extract_error(response).await)
        }
    }

    pub async fn delete_routing_config(&self) -> Result<()> {
        let url = format!("{}/routing", self.host);

        let response = self
            .client
            .delete(&url)
            .bearer_auth(&self.token)
            .send()
            .await
            .context(RequestExecutionSnafu)?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(extract_error(response).await)
        }
    }
}
