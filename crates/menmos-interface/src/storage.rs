use std::collections::HashMap;
use std::fmt::Formatter;
use std::io;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;
use std::{fmt, fs};

use anyhow::Result;

use async_trait::async_trait;

use bytes::Bytes;

use futures::Stream;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

fn file_to_base64<P: AsRef<Path>>(path: P) -> Result<String> {
    Ok(base64::encode(fs::read(path.as_ref())?))
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
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
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BlobMetaRequest {
    /// The key/value pairs for this blob.
    pub fields: HashMap<String, FieldValue>,

    /// The tags for this blob.
    pub tags: Vec<String>,
}

impl BlobMetaRequest {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_field<S: Into<String>, T: Into<FieldValue>>(mut self, key: S, value: T) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }

    #[must_use]
    pub fn with_tag<S: Into<String>>(mut self, s: S) -> Self {
        self.tags.push(s.into());
        self
    }

    pub fn into_meta(
        self,
        created_at: OffsetDateTime,
        modified_at: OffsetDateTime,
        size: u64,
    ) -> BlobMeta {
        BlobMeta {
            fields: self.fields,
            tags: self.tags,
            size,
            created_at,
            modified_at,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Hash, Serialize, PartialEq, Eq, Ord, PartialOrd)]
#[serde(untagged)]
pub enum FieldValue {
    Str(String),
    Numeric(i64),
}

#[derive(Clone, Debug, Deserialize, Hash, Serialize, PartialEq, Eq)]
pub enum TaggedFieldValue {
    Str(String),
    Numeric(i64),
}

impl From<FieldValue> for TaggedFieldValue {
    fn from(v: FieldValue) -> Self {
        match v {
            FieldValue::Str(s) => TaggedFieldValue::Str(s),
            FieldValue::Numeric(i) => TaggedFieldValue::Numeric(i),
        }
    }
}

impl From<TaggedFieldValue> for FieldValue {
    fn from(v: TaggedFieldValue) -> Self {
        match v {
            TaggedFieldValue::Str(s) => FieldValue::Str(s),
            TaggedFieldValue::Numeric(i) => FieldValue::Numeric(i),
        }
    }
}

impl From<String> for FieldValue {
    fn from(v: String) -> Self {
        Self::Str(v)
    }
}

impl From<&str> for FieldValue {
    fn from(v: &str) -> Self {
        Self::Str(String::from(v))
    }
}

impl From<&String> for FieldValue {
    fn from(v: &String) -> Self {
        Self::Str(String::from(v))
    }
}

impl From<i64> for FieldValue {
    fn from(v: i64) -> Self {
        Self::Numeric(v)
    }
}

impl fmt::Display for FieldValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Str(s) => write!(f, "\"{}\"", s),
            Self::Numeric(i) => write!(f, "{}", i),
        }
    }
}

/// Metadata associated with a blob.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BlobMeta {
    /// The key/value pairs for this blob.
    pub fields: HashMap<String, FieldValue>,

    /// The tags for this blob.
    pub tags: Vec<String>,

    /// This blob's size, in bytes.
    pub size: u64,

    /// This blob's creation time.
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,

    /// This blob's last modified time.
    #[serde(with = "time::serde::rfc3339")]
    pub modified_at: OffsetDateTime,
}

impl From<BlobMeta> for BlobMetaRequest {
    fn from(m: BlobMeta) -> Self {
        Self {
            fields: m.fields,
            tags: m.tags,
        }
    }
}

impl Default for BlobMeta {
    fn default() -> Self {
        Self {
            fields: Default::default(),
            tags: Default::default(),
            size: 0,
            created_at: OffsetDateTime::now_utc(),
            modified_at: OffsetDateTime::now_utc(),
        }
    }
}

impl BlobMeta {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_field<S: Into<String>, T: Into<FieldValue>>(mut self, key: S, value: T) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }

    #[must_use]
    pub fn with_tag<S: Into<String>>(mut self, s: S) -> Self {
        self.tags.push(s.into());
        self
    }

    #[must_use]
    pub fn with_size(mut self, size: u64) -> Self {
        self.size = size;
        self
    }
}

/// Tagged version of the metadata associated with a blob.
///
/// This is used to persist the metadata in the sled tree.
/// bincode doesn't like untagged enums, so we have to make a tagged alternative.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaggedBlobMeta {
    /// The key/value pairs for this blob.
    pub fields: HashMap<String, TaggedFieldValue>,

    /// The tags for this blob.
    pub tags: Vec<String>,

    /// This blob's size, in bytes.
    pub size: u64,

    /// This blob's creation time.
    #[serde(with = "crate::timestamp_nanos")]
    pub created_at: OffsetDateTime,

    /// This blob's last modified time.
    #[serde(with = "crate::timestamp_nanos")]
    pub modified_at: OffsetDateTime,
}

impl From<BlobMeta> for TaggedBlobMeta {
    fn from(m: BlobMeta) -> Self {
        Self {
            fields: m
                .fields
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect::<HashMap<_, _>>(),
            tags: m.tags,
            size: m.size,
            created_at: m.created_at,
            modified_at: m.modified_at,
        }
    }
}

impl From<TaggedBlobMeta> for BlobMeta {
    fn from(m: TaggedBlobMeta) -> Self {
        Self {
            fields: m
                .fields
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect::<HashMap<_, _>>(),
            tags: m.tags,
            size: m.size,
            created_at: m.created_at,
            modified_at: m.modified_at,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BlobInfoRequest {
    pub meta_request: BlobMetaRequest,
    pub owner: String,
    pub size: u64,
}

impl BlobInfoRequest {
    pub fn into_blob_info(
        self,
        created_at: OffsetDateTime,
        modified_at: OffsetDateTime,
    ) -> BlobInfo {
        BlobInfo {
            meta: self
                .meta_request
                .into_meta(created_at, modified_at, self.size),
            owner: self.owner,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BlobInfo {
    pub meta: BlobMeta,
    pub owner: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaggedBlobInfo {
    pub meta: TaggedBlobMeta,
    pub owner: String,
}

impl From<BlobInfo> for TaggedBlobInfo {
    fn from(v: BlobInfo) -> Self {
        Self {
            meta: v.meta.into(),
            owner: v.owner,
        }
    }
}

impl From<TaggedBlobInfo> for BlobInfo {
    fn from(v: TaggedBlobInfo) -> Self {
        Self {
            meta: v.meta.into(),
            owner: v.owner,
        }
    }
}

pub struct Blob {
    pub stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send>,
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

pub type DynStorageNode = Arc<dyn StorageNode + Send + Sync>;
