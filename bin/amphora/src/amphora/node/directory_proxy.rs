use std::fs;
use std::io::Write;
use std::path::Path;

use anyhow::{ensure, Result};

use reqwest::header;

use interface::{
    message::directory_node::{CertificateInfo, RegisterResponse},
    BlobMeta, StorageNodeInfo,
};

use reqwest::Url;

use super::constants;
use crate::DirectoryHostConfig;

pub struct RegisterResponseWrapper {
    pub certificate_info: Option<CertificateInfo>,
    pub rebuild_requested: bool,
}

pub struct DirectoryProxy {
    client: reqwest::Client,

    directory_url: Url,
    registration_secret: String,
}

impl DirectoryProxy {
    pub fn new(cfg: &DirectoryHostConfig, registration_secret: String) -> Result<Self> {
        let url = Url::parse(&format!("{}:{}", &cfg.url, cfg.port))?;
        Ok(Self {
            client: reqwest::Client::new(),
            directory_url: url,
            registration_secret,
        })
    }

    pub async fn register_storage_node(
        &self,
        def: StorageNodeInfo,
        certificate_path: &Path,
    ) -> Result<RegisterResponseWrapper> {
        let url = self.directory_url.join("node/storage")?;
        let req = self
            .client
            .put(url)
            .json(&def)
            .header(
                header::HeaderName::from_static("x-registration-secret"),
                self.registration_secret.clone(),
            )
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
                fs::File::create(certificate_path.join(constants::CERTIFICATE_FILE_NAME))?;
            cert.write_all(&base64::decode(&certs.certificate_b64)?)?;

            // Write the private key.
            let mut key =
                fs::File::create(certificate_path.join(constants::PRIVATE_KEY_FILE_NAME))?;
            key.write_all(&base64::decode(&certs.private_key_b64)?)?;
        }

        Ok(RegisterResponseWrapper {
            certificate_info: response.certificates,
            rebuild_requested: response.rebuild_requested,
        })
    }

    pub async fn rebuild_complete(
        &self,
        storage_node_id: &str,
        admin_password: &str,
    ) -> Result<()> {
        let url = self
            .directory_url
            .join(&format!("rebuild/{}", storage_node_id))?;

        let req = self
            .client
            .delete(url)
            .header("authorization", admin_password)
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

    pub async fn index_blob(
        &self,
        blob_id: &str,
        blob_meta: BlobMeta,
        storage_node_id: &str,
    ) -> Result<()> {
        let url = self
            .directory_url
            .join(&format!("blob/{}/metadata", blob_id))?;

        let req = self
            .client
            .put(url)
            .json(&blob_meta)
            .header(
                header::HeaderName::from_static("x-storage-id"),
                storage_node_id.to_string(),
            )
            .header(
                header::HeaderName::from_static("x-registration-secret"),
                self.registration_secret.clone(),
            )
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
