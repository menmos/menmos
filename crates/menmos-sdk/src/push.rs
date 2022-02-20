use std::path::PathBuf;

use menmos_client::{ClientError, Meta};

use snafu::prelude::*;

use crate::error;
use crate::metadata_detector::MetadataDetectorRC;
use crate::{ClientRC, UploadRequest};

#[derive(Debug, Snafu)]
pub enum PushError {
    MetadataPopulationError {
        source: error::MetadataDetectorError,
    },
    #[snafu(display("failed to push '{:?}'", path))]
    BlobPushError { path: PathBuf, source: ClientError },
}

type Result<T> = std::result::Result<T, PushError>;

pub struct PushResult {
    pub source_path: PathBuf,
    pub blob_id: String,
}

pub(crate) async fn push_file(
    client: ClientRC,
    metadata_detector: &MetadataDetectorRC,
    request: UploadRequest,
) -> Result<String> {
    let mut meta = Meta::new();

    meta = meta.with_field(
        "name",
        request
            .path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string(),
    );

    metadata_detector
        .populate(&request.path, &mut meta)
        .context(MetadataPopulationSnafu)?;

    for tag in request.tags.iter() {
        meta = meta.with_tag(tag);
    }

    for (k, v) in request.fields.iter() {
        meta = meta.with_field(k, v.clone());
    }

    let item_id = client
        .push(&request.path, meta)
        .await
        .with_context(|_| BlobPushSnafu {
            path: request.path.clone(),
        })?;

    Ok(item_id)
}
