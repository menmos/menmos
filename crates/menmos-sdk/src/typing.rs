use interface::FieldValue;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use menmos_client::Client;

pub type ClientRC = Arc<Client>;

/// The metadata of a blob.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FileMetadata {
    /// The key/value pairs for this file.
    pub fields: HashMap<String, FieldValue>,

    /// The tags for this file.
    pub tags: Vec<String>,
}

impl FileMetadata {
    pub fn new<S: Into<String>>(name: S) -> Self {
        let mut fields: HashMap<String, FieldValue> = HashMap::new();
        fields.insert("name".into(), FieldValue::Str(name.into()));
        Self {
            fields,
            ..Default::default()
        }
    }

    #[must_use]
    pub fn with_tag<S: Into<String>>(mut self, tag: S) -> Self {
        self.tags.push(tag.into());
        self
    }

    #[must_use]
    pub fn with_field<K: Into<String>, V: Into<FieldValue>>(mut self, key: K, value: V) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadRequest {
    /// The path of the file to upload.
    pub path: PathBuf,

    /// The metadata of the file to upload.
    pub fields: HashMap<String, String>,

    /// The tags of the file to upload.
    pub tags: Vec<String>,
}
