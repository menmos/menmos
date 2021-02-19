use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

fn default_group_by_tags() -> bool {
    false
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum VirtualDirectoryFilter {
    Tag { key: String },
    Key { key: String },
    KeyValue { key: String, value: String },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Contents {
    Virtual(HashMap<String, Contents>),
    Root {
        root: String,
    },
    Query {
        expression: String,

        #[serde(default = "default_group_by_tags")]
        group_by_tags: bool,

        #[serde(default = "Vec::default")]
        group_by_meta_keys: Vec<String>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ClientConfig {
    Host { host: String, password: String },
    Profile { profile: String },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Mount {
    pub name: String,
    pub client: ClientConfig,
    pub mount_point: PathBuf,
    pub contents: Contents,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub mount: Mount,
}
