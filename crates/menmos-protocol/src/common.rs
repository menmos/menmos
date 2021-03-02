use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VersionResponse {
    pub version: String,
}

impl VersionResponse {
    pub fn new<V: Into<String>>(version: V) -> Self {
        Self {
            version: version.into(),
        }
    }
}
