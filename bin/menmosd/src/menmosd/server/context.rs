use std::sync::Arc;

use interface::{CertificateInfo, DirectoryNode};

use crate::Config;

#[derive(Clone)]
pub struct Context {
    pub certificate_info: Arc<Option<CertificateInfo>>,
    pub config: Arc<Config>,
    pub node: Arc<Box<dyn DirectoryNode + Send + Sync>>,
}
