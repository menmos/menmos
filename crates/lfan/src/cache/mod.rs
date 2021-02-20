#[cfg(feature = "async")]
mod concurrent_cache;

mod cache_trait;
mod modular_cache;
mod policy;
mod ttl_cache;

pub use cache_trait::Cache;

#[cfg(feature = "async")]
pub use concurrent_cache::ConcurrentCache;

pub use modular_cache::ModularCache;
pub use policy::{EvictionPolicy, InsertionPolicy};
pub use ttl_cache::TTLCache;
