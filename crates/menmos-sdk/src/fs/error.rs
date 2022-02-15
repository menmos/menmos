use snafu::prelude::*;
use std::string::FromUtf8Error;

use crate::util;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum FsError {
    // TODO: add source: ClientError once its exposed in menmos-client >= 0.1.0
    FileCreateError,

    // TODO: add source: ClientError once its exposed in menmos-client >= 0.1.0
    #[snafu(display("failed to delete blob '{}'", blob_id))]
    BlobDeleteError {
        blob_id: String,
    },

    #[snafu(display("failed to open file '{}': {}", blob_id, source))]
    FileOpenError {
        source: util::UtilError,
        blob_id: String,
    },

    // TODO: add source: ClientError once its exposed in menmos-client >= 0.1.0
    #[snafu(display("failed to write to file"))]
    FileWriteError,

    // TODO: add source: ClientError once its exposed in menmos-client >= 0.1.0
    #[snafu(display("failed to read from file '{}'", blob_id))]
    FileReadError {
        blob_id: String,
    },

    #[snafu(display("failed to remove file '{}': {}", blob_id, source))]
    FileRemoveError {
        source: util::UtilError,
        blob_id: String,
    },

    // TODO: add source: ClientError once its exposed in menmos-client >= 0.1.0
    #[snafu(display("failed to create directory"))]
    DirCreateError,

    // TODO: add source: ClientError once its exposed in menmos-client >= 0.1.0
    #[snafu(display("failed to list directory"))]
    DirListError,

    #[snafu(display("failed to query directory"))]
    DirQueryError {
        source: util::UtilError,
    },

    #[snafu(display("failed to remove directory: {}", source))]
    DirRemoveError {
        source: util::UtilError,
    },

    #[snafu(display("failed to get blob size for seeking"))]
    SeekMetaError {
        source: util::UtilError,
    },

    #[snafu(display("seek reached a negative offset"))]
    NegativeOffsetError,

    #[snafu(display("buffer value is not valid UTF-8"))]
    BufferEncodingError {
        source: FromUtf8Error,
    },
}

pub type Result<T> = std::result::Result<T, FsError>;
