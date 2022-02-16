pub mod fs;
mod metadata_detector;
mod profile;
pub mod push;
mod typing;
mod util;

pub use menmos_client::{BuildError, Query};
pub use profile::{Config, Profile};
pub use typing::{FileMetadata, UploadRequest};

use metadata_detector::{MetadataDetector, MetadataDetectorRC};
use typing::*;

use std::sync::Arc;
use std::time;

use async_stream::try_stream;

use futures::{TryStream, TryStreamExt};
use interface::Hit;

pub use interface;

use menmos_client::Client;

use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum MenmosError {
    ConfigLoad {
        source: error::ProfileError,
    },

    #[snafu(display("profile '{}' does not exist", profile))]
    ProfileLoad {
        profile: String,
    },

    #[snafu(display("failed to build client"))]
    ClientBuild {
        source: BuildError,
    },

    FilePush {
        source: error::PushError,
    },

    DirectoryRead {
        source: std::io::Error,
    },

    Query {
        source: util::UtilError,
    },
}

type Result<T> = std::result::Result<T, MenmosError>;

mod error {
    pub use super::MenmosError;
    pub use crate::fs::FsError;
    pub use crate::metadata_detector::MetadataDetectorError;
    pub use crate::profile::ProfileError;
    pub use crate::push::PushError;
}

fn load_profile_from_config(profile: &str) -> Result<Profile> {
    let config = Config::load().context(ConfigLoadSnafu)?;
    config
        .profiles
        .get(profile)
        .cloned()
        .context(ProfileLoadSnafu {
            profile: String::from(profile),
        })
}

/// The menmos client.
#[derive(Clone)]
pub struct Menmos {
    /// The filesystem interface to menmos.
    ///
    /// This interface should be used when manipulating concepts that are similar to files and folders.
    pub fs: fs::MenmosFs,

    client: ClientRC,

    metadata_detector: MetadataDetectorRC,
}

impl Menmos {
    fn new_with_client(client: Client) -> Self {
        let client_rc = Arc::new(client);
        let fs = fs::MenmosFs::new(client_rc.clone());

        // If this fails we shipped a bad library.
        let metadata_detector = Arc::new(MetadataDetector::new().unwrap());

        Self {
            fs,
            client: client_rc,
            metadata_detector,
        }
    }

    pub async fn new(profile: &str) -> Result<Self> {
        let profile = load_profile_from_config(profile)?;
        let client = Client::builder()
            .with_host(profile.host)
            .with_username(profile.username)
            .with_password(profile.password)
            .build()
            .await
            .context(ClientBuildSnafu)?;
        Ok(Self::new_with_client(client))
    }

    /// Get a builder to configure the client.
    pub fn builder(profile: &str) -> MenmosBuilder {
        MenmosBuilder::new(profile.into())
    }

    /// Get a reference to the internal low-level menmos client.
    pub fn client(&self) -> &Client {
        self.client.as_ref()
    }

    /// Get a stream of results for a given query.
    pub fn query(&self, query: Query) -> impl TryStream<Ok = Hit, Error = MenmosError> + Unpin {
        util::scroll_query(query, &self.client).map_err(|e| MenmosError::Query { source: e })
    }

    /// Recursively push a sequence of files and/or directories to the menmos cluster.
    pub fn push_files(
        &self,
        requests: Vec<UploadRequest>,
    ) -> impl TryStream<Ok = push::PushResult, Error = MenmosError> + Unpin {
        let client = self.client.clone();
        let metadata_detector = self.metadata_detector.clone();

        Box::pin(try_stream! {
            let mut working_stack = Vec::new();
            working_stack.extend(requests);

            while let Some(upload_request) = working_stack.pop(){
                if upload_request.path.is_file() {
                    let source_path = upload_request.path.clone();
                    let blob_id = push::push_file(client.clone(), &metadata_detector, upload_request).await.map_err(|e| MenmosError::FilePush{source: e})?;
                    yield push::PushResult{source_path, blob_id};
                } else {
                    let directory_id: String = push::push_file(
                        client.clone(),
                        &metadata_detector,
                        upload_request.clone()                    )
                    .await.context(FilePushSnafu)?;

                    // Add this directory's children to the working stack.
                    let read_dir_result: Result<std::fs::ReadDir> = upload_request.path.read_dir().map_err(|e| MenmosError::DirectoryRead{source: e});
                    for child in read_dir_result?.filter_map(|f| f.ok()) {
                        let mut req_clone = upload_request.clone();
                        req_clone.path = child.path().clone();
                        req_clone.fields.insert("parent".to_string(), directory_id.clone());
                        working_stack.push(req_clone);
                    }
                }
            }
        })
    }
}

pub struct MenmosBuilder {
    profile: String,
    request_timeout: Option<time::Duration>,
    max_retry_count: Option<usize>,
    retry_interval: Option<time::Duration>,
}

impl MenmosBuilder {
    pub(crate) fn new(profile: String) -> Self {
        Self {
            profile,
            request_timeout: None,
            max_retry_count: None,
            retry_interval: None,
        }
    }

    #[must_use]
    pub fn with_request_timeout(mut self, request_timeout: time::Duration) -> Self {
        self.request_timeout = Some(request_timeout);
        self
    }

    #[must_use]
    pub fn with_max_retry_count(mut self, max_retry_count: usize) -> Self {
        self.max_retry_count = Some(max_retry_count);
        self
    }

    #[must_use]
    pub fn with_retry_interval(mut self, retry_interval: time::Duration) -> Self {
        self.retry_interval = Some(retry_interval);
        self
    }

    pub async fn build(self) -> Result<Menmos> {
        let profile = load_profile_from_config(&self.profile)?;
        let mut builder = Client::builder()
            .with_host(profile.host)
            .with_username(profile.username)
            .with_password(profile.password);

        if let Some(request_timeout) = self.request_timeout {
            builder = builder.with_request_timeout(request_timeout);
        }

        if let Some(max_retry_count) = self.max_retry_count {
            builder = builder.with_max_retry_count(max_retry_count);
        }

        if let Some(retry_interval) = self.retry_interval {
            builder = builder.with_retry_interval(retry_interval);
        }

        let client = builder.build().await.context(ClientBuildSnafu)?;

        Ok(Menmos::new_with_client(client))
    }
}
