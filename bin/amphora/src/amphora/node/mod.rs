use anyhow::Result;

mod concurrent_repository;
pub mod constants;
mod directory_proxy;
mod index;
mod node_impl;
mod node_info;
mod rebuild;
mod stringlock;

use interface::CertificateInfo;
pub use node_impl::Storage;

use crate::{BlobStorageImpl, Config};
use concurrent_repository::ConcurrentRepository;

pub async fn make_node(cfg: Config, certs: Option<CertificateInfo>) -> Result<Storage> {
    let repo: Box<dyn repository::Repository + Send + Sync> = match &cfg.node.blob_storage {
        BlobStorageImpl::Directory { path } => {
            let r = repository::DiskRepository::new(path)?;
            Box::from(r)
        }
        BlobStorageImpl::S3 {
            bucket,
            cache_path,
            cache_size,
        } => {
            let r = repository::S3Repository::new(bucket, cache_path, *cache_size)?;
            Box::from(r)
        }
    };

    let node = Storage::new(cfg, repo, certs).await?;

    Ok(node)
}
