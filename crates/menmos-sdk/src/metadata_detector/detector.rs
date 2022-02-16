use std::{path::Path, sync::Arc};

use menmos_client::Meta;

use menmos_std::fs::mimetype;

use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum MetadataDetectorError {
    #[snafu(display("failed to deserialize mimetype data: {}", source))]
    MimetypeJsonError { source: serde_json::Error },
}

type Result<T> = std::result::Result<T, MetadataDetectorError>;

#[derive(Clone)]
pub struct MetadataDetector {}

impl MetadataDetector {
    pub fn new() -> Result<Self> {
        Ok(Self {})
    }

    pub fn populate<P: AsRef<Path>>(&self, path: P, meta: &mut Meta) -> Result<()> {
        if let Some(mime_type) = mimetype(&path) {
            meta.fields.insert(String::from("content-type"), mime_type);
        }

        if let Some(extension) = path.as_ref().extension().and_then(|e| e.to_str()) {
            meta.fields
                .insert(String::from("extension"), String::from(extension));
        }

        Ok(())
    }
}

pub type MetadataDetectorRC = Arc<MetadataDetector>;
