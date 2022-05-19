mod disk;
mod iface;
mod s3;
pub mod util;

pub use iface::{OperationGuard, Repository};

pub use disk::DiskRepository;
pub use s3::S3Repository;

pub use betterstreams::ChunkedStreamInfo as StreamInfo;
