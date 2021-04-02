use std::{collections::HashMap, path::Path};

use serde_json;
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum SmartDetectorError {
    #[snafu(display("failed to load the mime types: {}", source))]
    MimeTypesDeserializationError { source: serde_json::Error },
}

#[derive(Clone)]
pub struct SmartDetector {
    mime_types: HashMap<String, String>,
}

impl SmartDetector {
    pub fn new() -> Result<Self, SmartDetectorError> {
        let bytes = include_bytes!("data/mime-types.json");

        let mime_types = serde_json::from_slice(bytes).context(MimeTypesDeserializationError)?;

        Ok(Self { mime_types })
    }

    pub fn detect<P: AsRef<Path>>(&self, path: P) -> Option<String> {
        let ext = path.as_ref().extension()?;
        let ext_str = ext.to_string_lossy().to_string();

        let mime_type = self.mime_types.get(&ext_str)?;

        Some(mime_type.clone())
    }
}
