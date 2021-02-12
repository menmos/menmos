use std::path::{Path, PathBuf};
use std::time::Duration;

use apikit::payload::ErrorResponse;

use bytes::{Buf, Bytes};

use futures::{Stream, TryStreamExt};

use interface::{
    message::{directory_node::Query, storage_node},
    BlobMeta, GetMetaResponse, QueryResponse,
};

use hyper::{header, StatusCode};

use mpart_async::client::MultipartRequest;

use reqwest::Client as ReqwestClient;

use reqwest::Body;

use serde::de::DeserializeOwned;
use snafu::{ensure, ResultExt, Snafu};

use crate::{profile::ProfileError, ClientBuilder, Config, Parameters};

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
        meta: BlobMeta,
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

    #[snafu(display("unknown error"))]
    UnknownError,
}

fn encode_metadata(meta: BlobMeta) -> Result<String> {
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

pub struct Client {
    admin_password: String,
    client: ReqwestClient,
    host: String,
}

type Result<T> = std::result::Result<T, ClientError>;

impl Client {
    /// Create a new client with default settings.
    pub fn new<S: Into<String>, P: Into<String>>(
        directory_host: S,
        admin_password: P,
    ) -> Result<Self> {
        Client::new_with_params(Parameters {
            host: directory_host.into(),
            admin_password: admin_password.into(),
            pool_idle_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(60),
        })
    }

    pub fn new_with_profile<S: AsRef<str>>(profile: S) -> Result<Self> {
        let config = Config::load().context(ConfigLoadError)?;
        let profile = config
            .profiles
            .get(profile.as_ref())
            .ok_or(ClientError::MissingProfile {
                name: String::from(profile.as_ref()),
            })?;

        Self::new(profile.host.clone(), profile.password.clone())
    }

    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    pub(crate) fn new_with_params(params: Parameters) -> Result<Self> {
        let client = ReqwestClient::builder()
            .pool_idle_timeout(params.pool_idle_timeout)
            .timeout(params.request_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context(ClientBuildError)?;

        Ok(Self {
            host: params.host,
            admin_password: params.admin_password,
            client,
        })
    }

    fn prepare_push_request<P: AsRef<Path>>(
        &self,
        url: &str,
        path: P,
        encoded_meta: String,
    ) -> Result<reqwest::Request> {
        if path.as_ref().is_file() {
            let mut mpart = MultipartRequest::default();
            mpart.add_file("src", path.as_ref());

            self.client
                .post(url)
                .header(header::AUTHORIZATION, &self.admin_password)
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
                .header(header::AUTHORIZATION, &self.admin_password)
                .header(header::HeaderName::from_static("x-blob-meta"), encoded_meta)
                .build()
                .context(RequestBuildError)
        }
    }

    pub async fn create_empty(&self, meta: BlobMeta) -> Result<String> {
        let mut iter_count: u16 = 0;
        let mut url = format!("{}/blob", self.host);
        let meta_b64 = encode_metadata(meta)?;

        loop {
            ensure!(iter_count <= 10, RedirectLimitExceeded { limit: 10_u32 });
            iter_count += 1;

            let request = self
                .client
                .post(&url)
                .header(header::AUTHORIZATION, &self.admin_password)
                .header(
                    header::HeaderName::from_static("x-blob-meta"),
                    meta_b64.clone(),
                )
                .build()
                .context(RequestBuildError)?;

            let response = self
                .client
                .execute(request)
                .await
                .context(RequestExecutionError)?;

            let status = response.status();
            if status == StatusCode::TEMPORARY_REDIRECT {
                if let Some(new_location) = response.headers().get(header::LOCATION) {
                    let new_url = String::from_utf8_lossy(new_location.as_bytes());
                    url = new_url.to_string();
                    log::debug!("redirect to {}", url);
                }
            } else if status.is_success() {
                // Our upload got through.
                // Deserialize the body to get the content ID.
                let put_response: storage_node::PutResponse = extract_body(response).await?;
                return Ok(put_response.id);
            } else {
                // An error occurred.
                return Err(extract_error(response).await);
            }
        }
    }

    async fn push_internal<P: AsRef<Path>>(
        &self,
        path: P,
        meta: BlobMeta,
        base_url: String,
    ) -> Result<String> {
        ensure!(
            path.as_ref().exists(),
            FileDoesNotExist {
                path: PathBuf::from(path.as_ref())
            }
        );

        let mut iter_count: u16 = 0;
        let mut url = base_url;
        let meta_b64 = encode_metadata(meta)?;

        loop {
            ensure!(iter_count <= 10, RedirectLimitExceeded { limit: 10_u32 });
            iter_count += 1;

            let request = self.prepare_push_request(&url, path.as_ref(), meta_b64.clone())?;

            let response = self
                .client
                .execute(request)
                .await
                .context(RequestExecutionError)?;

            let status = response.status();
            if status == StatusCode::TEMPORARY_REDIRECT {
                if let Some(new_location) = response.headers().get(header::LOCATION) {
                    let new_url = String::from_utf8_lossy(new_location.as_bytes());
                    url = new_url.to_string();
                    log::debug!("redirect to {}", url);
                }
            } else if status.is_success() {
                // Our upload got through.
                // Deserialize the body to get the content ID.
                let put_response: storage_node::PutResponse = extract_body(response).await?;
                return Ok(put_response.id);
            } else {
                // An error occurred.
                return Err(extract_error(response).await);
            }
        }
    }

    pub async fn push<P: AsRef<Path>>(&self, path: P, meta: BlobMeta) -> Result<String> {
        self.push_internal(path, meta, format!("{}/blob", self.host))
            .await
    }

    pub async fn update_blob<P: AsRef<Path>>(
        &self,
        blob_id: &str,
        path: P,
        meta: BlobMeta,
    ) -> Result<String> {
        self.push_internal(path, meta, format!("{}/blob/{}", self.host, blob_id))
            .await
    }

    pub async fn update_meta(&self, blob_id: &str, meta: BlobMeta) -> Result<()> {
        let mut iter_count: u16 = 0;
        let mut url = format!("{}/blob/{}/metadata", self.host, blob_id);

        loop {
            ensure!(iter_count <= 10, RedirectLimitExceeded { limit: 10_u32 });
            iter_count += 1;

            let request = self
                .client
                .post(&url)
                .header(header::AUTHORIZATION, &self.admin_password)
                .json(&meta)
                .build()
                .context(RequestBuildError)?;

            let response = self
                .client
                .execute(request)
                .await
                .context(RequestExecutionError)?;

            let status = response.status();
            if status == StatusCode::TEMPORARY_REDIRECT {
                if let Some(new_location) = response.headers().get(header::LOCATION) {
                    let new_url = String::from_utf8_lossy(new_location.as_bytes());
                    url = new_url.to_string();
                    log::debug!("redirect to {}", url);
                }
            } else if status.is_success() {
                // Our upload got through.
                // Deserialize the body to get the content ID.
                return Ok(());
            } else {
                // An error occurred.
                return Err(extract_error(response).await);
            }
        }
    }

    pub async fn write<B: Buf>(&self, blob_id: &str, offset: u64, mut buffer: B) -> Result<()> {
        let mut iter_count: u16 = 0;
        let mut url = format!("{}/blob/{}", self.host, blob_id);

        let buffer_bytes = buffer.to_bytes();

        loop {
            ensure!(iter_count <= 10, RedirectLimitExceeded { limit: 10_u32 });
            iter_count += 1;

            let buffer_clone = buffer_bytes.clone();
            let stream = futures::stream::once(async move {
                let r: std::result::Result<Bytes, std::io::Error> = Ok(buffer_clone);
                r
            });
            let body = Body::wrap_stream(stream);

            let request = self
                .client
                .put(&url)
                .header(header::AUTHORIZATION, &self.admin_password)
                .header(
                    header::RANGE,
                    &format!(
                        "bytes={}-{}",
                        offset,
                        offset + (buffer_bytes.len() - 1) as u64
                    ),
                )
                .body(body)
                .build()
                .context(RequestBuildError)?;

            let response = self
                .client
                .execute(request)
                .await
                .context(RequestExecutionError)?;

            let status = response.status();
            if status == StatusCode::TEMPORARY_REDIRECT {
                if let Some(new_location) = response.headers().get(header::LOCATION) {
                    let new_url = String::from_utf8_lossy(new_location.as_bytes());
                    url = new_url.to_string();
                    log::debug!("redirect to {}", url);
                }
            } else if status.is_success() {
                // Our upload got through.
                // Deserialize the body to get the content ID.
                return Ok(());
            } else {
                // An error occurred.
                return Err(extract_error(response).await);
            }
        }
    }

    pub async fn get_meta(&self, blob_id: &str) -> Result<Option<BlobMeta>> {
        let url = format!("{}/blob/{}/metadata", self.host, blob_id);

        let request = self
            .client
            .get(&url)
            .header(header::AUTHORIZATION, &self.admin_password)
            .build()
            .context(RequestBuildError)?;

        let response = self
            .client
            .execute(request)
            .await
            .context(RequestExecutionError)?;

        let status = response.status();

        if status.is_success() {
            let resp: GetMetaResponse = extract_body(response).await?;
            Ok(resp.meta)
        } else {
            Err(extract_error(response).await)
        }
    }

    pub async fn get_file(&self, blob_id: &str) -> Result<impl Stream<Item = Result<Bytes>>> {
        let mut iter_count: u16 = 0;
        let mut url = format!("{}/blob/{}", self.host, blob_id);

        loop {
            ensure!(iter_count <= 10, RedirectLimitExceeded { limit: 10_u32 });
            iter_count += 1;

            let request = self
                .client
                .get(&url)
                .header(header::AUTHORIZATION, &self.admin_password)
                .build()
                .context(RequestBuildError)?;

            let response = self
                .client
                .execute(request)
                .await
                .context(RequestExecutionError)?;

            let status = response.status();
            if status == StatusCode::TEMPORARY_REDIRECT {
                if let Some(new_location) = response.headers().get(header::LOCATION) {
                    let new_url = String::from_utf8_lossy(new_location.as_bytes());
                    url = new_url.to_string();
                    log::debug!("redirect to {}", url);
                }
            } else if status.is_success() {
                // Our upload got through.
                // Deserialize the body to get the content ID.
                return Ok(response
                    .bytes_stream()
                    .map_err(|_| ClientError::UnknownError));
            } else {
                // An error occurred.
                return Err(extract_error(response).await);
            }
        }
    }

    // TODO: This API might be improved by using a bytes buffer instead of a raw vec.
    // TODO: Use a rust range instead of a tuple
    pub async fn read_range(&self, blob_id: &str, range: (u64, u64)) -> Result<Vec<u8>> {
        let mut iter_count: u16 = 0;
        let mut url = format!("{}/blob/{}", self.host, blob_id);

        loop {
            ensure!(iter_count <= 10, RedirectLimitExceeded { limit: 10_u32 });
            iter_count += 1;

            let request = self
                .client
                .get(&url)
                .header(header::AUTHORIZATION, &self.admin_password)
                .header(header::RANGE, &format!("bytes={}-{}", range.0, range.1))
                .build()
                .context(RequestBuildError)?;

            let response = self
                .client
                .execute(request)
                .await
                .context(RequestExecutionError)?;

            let status = response.status();
            if status == StatusCode::TEMPORARY_REDIRECT {
                if let Some(new_location) = response.headers().get(header::LOCATION) {
                    let new_url = String::from_utf8_lossy(new_location.as_bytes());
                    url = new_url.to_string();
                    log::debug!("redirect to {}", url);
                }
            } else if status.is_success() {
                // Our upload got through.
                // Deserialize the body to get the content ID.
                let resp_bytes = response.bytes().await.context(FetchBodyError)?;
                return Ok(resp_bytes.to_vec());
            } else {
                // An error occurred.
                return Err(extract_error(response).await);
            }
        }
    }

    pub async fn query(&self, query: Query) -> Result<QueryResponse> {
        let url = format!("{}/query", self.host);

        let request = self
            .client
            .post(&url)
            .header(header::AUTHORIZATION, &self.admin_password)
            .json(&query)
            .build()
            .context(RequestBuildError)?;

        let response = self
            .client
            .execute(request)
            .await
            .context(RequestExecutionError)?;

        let status = response.status();

        if status.is_success() {
            let query_response: QueryResponse = extract_body(response).await?;
            Ok(query_response)
        } else {
            Err(extract_error(response).await)
        }
    }

    pub async fn delete(&self, blob_id: String) -> Result<()> {
        let mut url = format!("{}/blob/{}", self.host, blob_id);
        let mut iter_count: u16 = 0;

        loop {
            ensure!(iter_count <= 10, RedirectLimitExceeded { limit: 10_u32 });
            iter_count += 1;

            let request = self
                .client
                .delete(&url)
                .header(header::AUTHORIZATION, &self.admin_password)
                .build()
                .context(RequestBuildError)?;

            let response = self
                .client
                .execute(request)
                .await
                .context(RequestExecutionError)?;
            println!("GOT RESPONSE");

            let status = response.status();
            if status == StatusCode::TEMPORARY_REDIRECT {
                println!("GOT REDIRECT STATUS");
                if let Some(new_location) = response.headers().get(header::LOCATION) {
                    let new_url = String::from_utf8_lossy(new_location.as_bytes());
                    url = new_url.to_string();
                    log::debug!("redirect to {}", url);
                    println!("redirect to {}", url);
                }
            } else if status.is_success() {
                // Our delete got through.
                return Ok(());
            } else {
                // An error occurred.
                return Err(extract_error(response).await);
            }
        }
    }
}
