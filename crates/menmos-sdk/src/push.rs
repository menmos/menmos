use std::path::PathBuf;

use menmos_client::Meta;

use snafu::prelude::*;

use crate::error;
use crate::metadata_detector::MetadataDetectorRC;
use crate::{ClientRC, UploadRequest};

#[derive(Debug, Snafu)]
pub enum PushError {
    MetadataPopulationError {
        source: error::MetadataDetectorError,
    },
    // TODO: add source: ClientError once its exposed in menmos-client >= 0.1.0
    #[snafu(display("failed to push '{:?}'", path))]
    BlobPushError { path: PathBuf },
}

type Result<T> = std::result::Result<T, PushError>;

pub struct PushResult {
    pub source_path: PathBuf,
    pub blob_id: String,
    pub parent_id: Option<String>,
}

pub(crate) async fn push_file(
    client: ClientRC,
    metadata_detector: &MetadataDetectorRC,
    blob_type: Type,
    request: UploadRequest,
) -> Result<String> {
    let mut meta = Meta::new(
        request
            .path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string(),
        blob_type.clone(),
    );

    metadata_detector
        .populate(&request.path, &mut meta)
        .context(MetadataPopulationSnafu)?;

    if blob_type == Type::File {
        meta = meta.with_size(request.path.metadata().unwrap().len())
    }

    if let Some(parent) = request.parent_id {
        meta = meta.with_parent(parent);
    }

    for tag in request.tags.iter() {
        meta = meta.with_tag(tag);
    }

    for (k, v) in request.metadata.iter() {
        meta = meta.with_meta(k, v);
    }

    let item_id = client
        .push(&request.path, meta)
        .await
        .map_err(|_| PushError::BlobPushError {
            path: request.path.clone(),
        })?;

    Ok(item_id)
}
