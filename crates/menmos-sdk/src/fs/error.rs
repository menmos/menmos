use std::string::FromUtf8Error;

use menmos_client::ClientError;

use snafu::prelude::*;

use crate::util;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum FsError {
    FileCreateError {
        source: ClientError,
    },

    #[snafu(display("failed to delete blob '{}'", blob_id))]
    BlobDeleteError {
        source: ClientError,
        blob_id: String,
    },

    #[snafu(display("failed to open file '{}': {}", blob_id, source))]
    FileOpenError {
        source: util::UtilError,
        blob_id: String,
    },

    #[snafu(display("failed to write to file"))]
    FileWriteError {
        source: ClientError,
    },

    #[snafu(display("failed to read from file '{}'", blob_id))]
    FileReadError {
        source: ClientError,
        blob_id: String,
    },

    #[snafu(display("failed to remove file '{}': {}", blob_id, source))]
    FileRemoveError {
        source: util::UtilError,
        blob_id: String,
    },

    #[snafu(display("failed to list children"))]
    DirListError {
        source: ClientError,
    },

    #[snafu(display("failed to query children"))]
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
