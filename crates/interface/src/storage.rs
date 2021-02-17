use std::io;
use std::{collections::HashMap, ops::Bound};

use anyhow::{ensure, Result};

use async_trait::async_trait;

use futures::Stream;

use headers::{Header, Range as HRange};

use reqwest::header::HeaderValue;

use serde::{Deserialize, Serialize};

use warp::hyper::body::Bytes;

use crate::message::directory_node::CertificateInfo;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum Type {
    File,
    Directory,
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

pub struct Blob {
    pub stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync>,
    pub current_chunk_size: u64,
    pub total_blob_size: u64,
    pub meta: BlobMeta,
}

#[derive(Clone, Debug)]
pub struct Range {
    pub start: Bound<u64>,
    pub end: Bound<u64>,
}

impl Range {
    pub fn from_header(header_value: HeaderValue) -> Result<Self> {
        // Decode the range string sent in the header value.
        let requested_ranges = HRange::decode(&mut vec![header_value].iter())?;

        // Convert the decoded range struct into a vectro of tuples of bounds.
        let ranges: Vec<(Bound<u64>, Bound<u64>)> = requested_ranges.iter().collect();

        ensure!(!ranges.is_empty(), "ranges cannot be empty");
        ensure!(
            ranges.len() == 1,
            "support for multipart range request is not implemented"
        );

        // Extract the bounds and return.
        let (start, end) = ranges[0];

        Ok(Self { start, end })
    }

    pub fn min_value(&self) -> Option<u64> {
        match self.start {
            Bound::Included(i) => Some(i),
            Bound::Excluded(i) => Some(i + 1),
            Bound::Unbounded => None,
        }
    }

    pub fn max_value(&self) -> Option<u64> {
        match self.end {
            Bound::Included(i) => Some(i),
            Bound::Excluded(i) => Some(i - 1),
            Bound::Unbounded => None,
        }
    }

    pub fn get_offset_range(&self, size: u64) -> std::ops::Range<u64> {
        let start = self.min_value().unwrap_or(0);
        let end = start + size - 1;
        start..end
    }
}

#[async_trait]
pub trait StorageNode {
    async fn put(
        &self,
        id: String,
        meta: BlobMeta,
        stream: Option<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin>>,
    ) -> Result<()>;

    async fn write(&self, id: String, range: Range, bytes: Bytes) -> Result<()>;

    async fn get(&self, blob_id: String, range: Option<Range>) -> Result<Blob>;

    async fn update_meta(&self, blob_id: String, meta: BlobMeta) -> Result<()>;

    async fn delete(&self, blob_id: String) -> Result<()>;

    async fn get_certificates(&self) -> Option<CertificateInfo>;

    async fn fsync(&self, blob_id: String) -> Result<()>;
}
