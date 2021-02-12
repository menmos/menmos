mod disk;
mod iface;
mod s3;

pub use iface::{Repository, StreamInfo};

pub use disk::DiskRepository;
pub use s3::S3Repository;
