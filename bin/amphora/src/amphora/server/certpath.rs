use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct CertPath {
    pub certificate: PathBuf,
    pub private_key: PathBuf,
}
