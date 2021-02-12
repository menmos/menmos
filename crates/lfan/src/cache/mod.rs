mod modular_cache;
mod policy;

pub use modular_cache::Cache;
pub use policy::{EvictionPolicy, InsertionPolicy};
