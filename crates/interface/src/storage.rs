use std::collections::HashMap;
use std::fs;
use std::io;
use std::ops::Bound;
use std::path::Path;

use anyhow::Result;

use async_trait::async_trait;

use futures::Stream;

use serde::{Deserialize, Serialize};

use warp::hyper::body::Bytes;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum Type {
    File,
    Directory,
}

fn file_to_base64<P: AsRef<Path>>(path: P) -> Result<String> {
    Ok(base64::encode(fs::read(path.as_ref())?))
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CertificateInfo {
    pub certificate_b64: String,
    pub private_key_b64: String,
}

impl CertificateInfo {
    pub fn from_path<P: AsRef<Path>, Q: AsRef<Path>>(
        certificate_path: P,
        private_key_path: Q,
    ) -> Result<CertificateInfo> {
        Ok(Self {
            certificate_b64: file_to_base64(certificate_path)?,
            private_key_b64: file_to_base64(private_key_path)?,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BlobMeta {
    pub name: String,
    pub blob_type: Type,
    pub metadata: HashMap<String, String>,
    pub tags: Vec<String>,
    pub parents: Vec<String>,
    pub size: u64,
}

impl BlobMeta {
    pub fn new<S: Into<String>>(name: S, blob_type: Type) -> Self {
        Self {
            name: name.into(),
            blob_type,
            metadata: Default::default(),
            tags: Default::default(),
            parents: Default::default(),
            size: 0,
        }
    }

    pub fn file<S: Into<String>>(name: S) -> Self {
        BlobMeta::new(name, Type::File)
    }

    pub fn directory<S: Into<String>>(name: S) -> Self {
        BlobMeta::new(name, Type::Directory)
    }

    pub fn with_meta<S: Into<String>, T: Into<String>>(mut self, key: S, value: T) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn with_tag<S: Into<String>>(mut self, s: S) -> Self {
        self.tags.push(s.into());
        self
    }

    pub fn with_parent<S: Into<String>>(mut self, s: S) -> Self {
        self.parents.push(s.into());
        self
    }

    pub fn with_size(mut self, size: u64) -> Self {
        self.size = size;
        self
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BlobInfo {
    pub meta: BlobMeta,
    pub owner: String,
}

pub struct Blob {
    pub stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync>,
    pub current_chunk_size: u64,
    pub total_blob_size: u64,
    pub info: BlobInfo,
}

#[async_trait]
pub trait StorageNode {
    async fn put(
        &self,
        id: String,
        info: BlobInfo,
        stream: Option<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin>>,
    ) -> Result<()>;

    async fn write(&self, id: String, range: (Bound<u64>, Bound<u64>), bytes: Bytes) -> Result<()>;

    async fn get(&self, blob_id: String, range: Option<(Bound<u64>, Bound<u64>)>) -> Result<Blob>;

    async fn update_meta(&self, blob_id: String, info: BlobInfo) -> Result<()>;

    async fn delete(&self, blob_id: String) -> Result<()>;

    async fn get_certificates(&self) -> Option<CertificateInfo>;

    async fn fsync(&self, blob_id: String) -> Result<()>;
}
