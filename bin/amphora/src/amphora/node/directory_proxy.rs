use std::path::Path;

use anyhow::{ensure, Result};

use http::Method;
use interface::{BlobInfo, CertificateInfo, StorageNodeInfo};

use opentelemetry::global;
use opentelemetry_http::HeaderInjector;

use protocol::directory::storage::{MoveRequest, RegisterResponse};

use reqwest::{IntoUrl, Url};

use tokio::fs;
use tokio::io::AsyncWriteExt;

use tracing_opentelemetry::OpenTelemetrySpanExt;

use super::constants;
use crate::DirectoryHostConfig;

pub struct RegisterResponseWrapper {
    pub certificate_info: Option<CertificateInfo>,
    pub rebuild_requested: bool,
    pub move_requests: Vec<MoveRequest>,
}

pub struct DirectoryProxy {
    client: reqwest::Client,

    directory_url: Url,
    encryption_key: String,
}

impl DirectoryProxy {
    pub fn new(cfg: &DirectoryHostConfig, encryption_key: &str) -> Result<Self> {
        let url = Url::parse(&format!("{}:{}", &cfg.url, cfg.port))?;
        Ok(Self {
            client: reqwest::Client::new(),
            directory_url: url,
            encryption_key: encryption_key.to_string(),
        })
    }

    fn get_token(&self, id: &str) -> Result<String> {
        menmos_auth::make_token(
            &self.encryption_key,
            menmos_auth::StorageNodeIdentity {
                id: String::from(id),
            },
        )
    }

    /// Build a request to the directory node and forward the current tracing context with it.
    fn make_request<U: IntoUrl>(
        &self,
        id: &str,
        method: Method,
        url: U,
    ) -> Result<reqwest::RequestBuilder> {
        let token = self.get_token(id)?;
        let builder = self.client.request(method, url).bearer_auth(token);

        let mut headers = reqwest::header::HeaderMap::new();

        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(
                &tracing::Span::current().context(),
                &mut HeaderInjector(&mut headers),
            );
        });

        Ok(builder.headers(headers))
    }

    #[tracing::instrument(skip(self, def, certificate_path))]
    pub async fn register_storage_node(
        &self,
        def: StorageNodeInfo,
        certificate_path: &Path,
    ) -> Result<RegisterResponseWrapper> {
        let url = self.directory_url.join("node/storage")?;
        let req = self
            .make_request(&def.id, Method::PUT, url)?
            .json(&def)
            .build()?;

        let resp = self.client.execute(req).await?;

        ensure!(
            resp.status().is_success(),
            format!(
                "Request failed: {}",
                String::from_utf8_lossy(resp.bytes().await?.as_ref())
            )
        );

        let body_bytes = resp.bytes().await?;
        let response: RegisterResponse = serde_json::from_slice(body_bytes.as_ref())?;

        // Write the certs on disk if need be.
        if let Some(certs) = &response.certificates {
            // Write the certificate.
            let mut cert =
                fs::File::create(certificate_path.join(constants::CERTIFICATE_FILE_NAME)).await?;
            cert.write_all(&base64::decode(&certs.certificate_b64)?)
                .await?;

            // Write the private key.
            let mut key =
                fs::File::create(certificate_path.join(constants::PRIVATE_KEY_FILE_NAME)).await?;
            key.write_all(&base64::decode(&certs.private_key_b64)?)
                .await?;
        }

        Ok(RegisterResponseWrapper {
            certificate_info: response.certificates,
            rebuild_requested: response.rebuild_requested,
            move_requests: response.move_requests,
        })
    }

    #[tracing::instrument(skip(self))]
    pub async fn rebuild_complete(&self, storage_node_id: &str) -> Result<()> {
        let url = self
            .directory_url
            .join(&format!("rebuild/{}", storage_node_id))?;

        let req = self
            .make_request(storage_node_id, Method::DELETE, url)?
            .build()?;

        let resp = self.client.execute(req).await?;

        ensure!(
            resp.status().is_success(),
            format!(
                "request failed: {}",
                String::from_utf8_lossy(resp.bytes().await?.as_ref())
            )
        );

        Ok(())
    }

    #[tracing::instrument(skip(self, blob_info))]
    pub async fn index_blob(
        &self,
        blob_id: &str,
        blob_info: BlobInfo,
        storage_node_id: &str,
    ) -> Result<()> {
        let url = self
            .directory_url
            .join(&format!("blob/{}/metadata", blob_id))?;

        let req = self
            .make_request(storage_node_id, Method::POST, url)?
            .json(&blob_info)
            .build()?;

        let resp = self.client.execute(req).await?;

        ensure!(
            resp.status().is_success(),
            format!(
                "request failed: {}",
                String::from_utf8_lossy(resp.bytes().await?.as_ref())
            )
        );

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn delete_blob(&self, blob_id: &str, storage_node_id: &str) -> Result<()> {
        let url = self
            .directory_url
            .join(&format!("blob/{}/metadata", blob_id))?;

        let req = self
            .make_request(storage_node_id, Method::DELETE, url)?
            .build()?;

        let resp = self.client.execute(req).await?;

        ensure!(
            resp.status().is_success(),
            format!(
                "request failed: {}",
                String::from_utf8_lossy(resp.bytes().await?.as_ref())
            )
        );

        Ok(())
    }
}
