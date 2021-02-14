#[cfg(feature = "async")]
mod concurrent_cache;

mod cache;
mod modular_cache;
mod policy;
mod ttl_cache;

#[cfg(feature = "async")]
pub use concurrent_cache::ConcurrentCache;

pub use modular_cache::ModularCache;
pub use policy::{EvictionPolicy, InsertionPolicy};
pub use ttl_cache::TTLCache;
