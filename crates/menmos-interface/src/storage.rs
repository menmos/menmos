use std::collections::HashMap;
use std::fs;
use std::io;
use std::ops::Bound;
use std::path::Path;

use anyhow::Result;

use async_trait::async_trait;

use chrono::{DateTime, Utc};

use futures::Stream;

use serde::{Deserialize, Serialize};

use warp::hyper::body::Bytes;

/// The type of a blob in a menmos cluster (file or directory).
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

/// Metadata accepted when indexing a blob.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BlobMetaRequest {
    /// The name of this blob. Does not need to be unique.
    pub name: String,

    /// The type of this blob.
    pub blob_type: Type,

    /// The key/value pairs for this blob.
    pub metadata: HashMap<String, String>,

    /// The tags for this blob.
    pub tags: Vec<String>,

    /// This blob's parent IDs.
    pub parents: Vec<String>,

    /// This blob's size, in bytes.
    pub size: u64,
}

impl BlobMetaRequest {
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
        Self::new(name, Type::File)
    }

    pub fn directory<S: Into<String>>(name: S) -> Self {
        Self::new(name, Type::Directory)
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

    pub fn into_meta(self, created_at: DateTime<Utc>, modified_at: DateTime<Utc>) -> BlobMeta {
        BlobMeta {
            name: self.name,
            blob_type: self.blob_type,
            metadata: self.metadata,
            tags: self.tags,
            parents: self.parents,
            size: self.size,
            created_at,
            modified_at,
        }
    }
}

/// Metadata associated with a blob.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BlobMeta {
    /// The name of this blob. Does not need to be unique.
    pub name: String,

    /// The type of this blob.
    pub blob_type: Type,

    /// The key/value pairs for this blob.
    pub metadata: HashMap<String, String>,

    /// The tags for this blob.
    pub tags: Vec<String>,

    /// This blob's parent IDs.
    pub parents: Vec<String>,

    /// This blob's size, in bytes.
    pub size: u64,

    /// This blob's creation time.
    pub created_at: DateTime<Utc>,

    /// This blob's last modified time.
    pub modified_at: DateTime<Utc>,
}

impl From<BlobMeta> for BlobMetaRequest {
    fn from(m: BlobMeta) -> Self {
        Self {
            name: m.name,
            blob_type: m.blob_type,
            metadata: m.metadata,
            tags: m.tags,
            parents: m.parents,
            size: m.size,
        }
    }
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
            created_at: Utc::now(),
            modified_at: Utc::now(),
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
pub struct BlobInfoRequest {
    pub meta_request: BlobMetaRequest,
    pub owner: String,
}

impl BlobInfoRequest {
    pub fn into_blob_info(self, created_at: DateTime<Utc>, modified_at: DateTime<Utc>) -> BlobInfo {
        BlobInfo {
            meta: self.meta_request.into_meta(created_at, modified_at),
            owner: self.owner,
        }
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
        info: BlobInfoRequest,
        stream: Option<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin>>,
    ) -> Result<()>;

    async fn write(
        &self,
        id: String,
        range: (Bound<u64>, Bound<u64>),
        bytes: Bytes,
        username: &str,
    ) -> Result<()>;

    async fn get(&self, blob_id: String, range: Option<(Bound<u64>, Bound<u64>)>) -> Result<Blob>;

    async fn update_meta(&self, blob_id: String, info: BlobInfoRequest) -> Result<()>;

    async fn delete(&self, blob_id: String, username: &str) -> Result<()>;

    async fn get_certificates(&self) -> Option<CertificateInfo>;

    async fn fsync(&self, blob_id: String, username: &str) -> Result<()>;

    async fn flush(&self) -> Result<()>;
}
