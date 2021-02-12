mod evict_lru;
mod insert_always;

pub mod insertion {
    pub use super::insert_always::AlwaysInsertPolicy;
}

pub mod eviction {
    pub use super::evict_lru::LRUEvictionPolicy;
}
