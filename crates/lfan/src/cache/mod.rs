#[cfg(feature = "async")]
mod concurrent_cache;

mod modular_cache;
mod policy;

#[cfg(feature = "async")]
pub use concurrent_cache::ConcurrentCache;

pub use modular_cache::Cache;
pub use policy::{EvictionPolicy, InsertionPolicy};
