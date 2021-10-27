use std::{collections::HashMap, path::Path};

use snafu::{ResultExt, Snafu};

use crate::Meta;

#[derive(Debug, Snafu)]
pub enum MetadataDetectorError {
    #[snafu(display("failed to load the mime types: {}", source))]
    MimeTypesDeserializationError { source: serde_json::Error },
}

type Result<T> = std::result::Result<T, MetadataDetectorError>;

#[derive(Clone)]
pub struct MetadataDetector {
    mime_types: HashMap<String, String>,
}

impl MetadataDetector {
    pub fn new() -> Result<Self> {
        let bytes = include_bytes!("data/mime-types.json");

        let mime_types = serde_json::from_slice(bytes).context(MimeTypesDeserializationError)?;

        Ok(Self { mime_types })
    }

    fn detect_mime_type<P: AsRef<Path>>(&self, path: P) -> Option<String> {
        let ext = path.as_ref().extension()?;
        let ext_str = ext.to_string_lossy().to_string();

        let mime_type = self.mime_types.get(&ext_str)?;

        Some(mime_type.to_owned())
    }

    pub fn populate<P: AsRef<Path>>(&self, path: P, meta: &mut Meta) -> Result<()> {
        if let Some(mime_type) = self.detect_mime_type(&path) {
            meta.metadata
                .insert(String::from("content-type"), mime_type);
        }

        if let Some(extension) = path.as_ref().extension().map(|e| e.to_str()).flatten() {
            meta.metadata
                .insert(String::from("extension"), String::from(extension));
        }

        Ok(())
    }
}

#[test]
fn detect_file_mime_type() {
    let path = "foo.txt";
    let metadata_detector = MetadataDetector::new().unwrap();

    let mime_type = metadata_detector.detect_mime_type(path);

    assert!(mime_type.is_some());
    assert_eq!(mime_type.unwrap(), "text/plain");
}

#[test]
fn detect_no_mime_type() {
    let path = "foo.invalid";
    let metadata_detector = MetadataDetector::new().unwrap();

    let mime_type = metadata_detector.detect_mime_type(path);

    assert!(mime_type.is_none());
}
