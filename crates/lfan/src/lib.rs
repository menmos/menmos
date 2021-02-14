mod cache;
mod policy;

pub use cache::{Cache, EvictionPolicy, InsertionPolicy};

pub mod preconfig {
    use super::policy::eviction::LRUEvictionPolicy;
    use super::policy::insertion::AlwaysInsertPolicy;
    use super::Cache;

    pub type LRUCache<K, V> = Cache<K, V, AlwaysInsertPolicy, LRUEvictionPolicy<K>>;

    #[cfg(feature = "async")]
    pub mod concurrent {

        use crate::cache::ConcurrentCache;
        use crate::policy::eviction::LRUEvictionPolicy;
        use crate::policy::insertion::AlwaysInsertPolicy;

        pub type LRUCache<K, V> = ConcurrentCache<K, V, AlwaysInsertPolicy, LRUEvictionPolicy<K>>;
    }
}
