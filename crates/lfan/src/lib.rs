mod cache;
mod policy;

pub use cache::{EvictionPolicy, InsertionPolicy, ModularCache, TTLCache};

pub mod preconfig {
    use super::policy::eviction::LRUEvictionPolicy;
    use super::policy::insertion::AlwaysInsertPolicy;
    use super::ModularCache;

    pub type LRUCache<K, V> = ModularCache<K, V, AlwaysInsertPolicy, LRUEvictionPolicy<K>>;
    pub type TTLLRUCache<K, V> = super::TTLCache<K, V, AlwaysInsertPolicy, LRUEvictionPolicy<K>>;

    #[cfg(feature = "async")]
    pub mod concurrent {
        use super::LRUCache as SingleThreadLRUCache;
        use super::TTLLRUCache as SingleThreadTTLCache;
        use crate::cache::ConcurrentCache;
        use std::{hash::Hash, time::Duration};

        pub type LRUCache<K, V> = ConcurrentCache<SingleThreadLRUCache<K, V>>;

        pub fn new_lru_cache<K, V>(maximum_size: usize) -> LRUCache<K, V>
        where
            K: Hash + Eq + Clone + std::fmt::Debug,
            V: Clone,
        {
            LRUCache::new(SingleThreadLRUCache::new(maximum_size))
        }

        pub type TTLLRUCache<K, V> = ConcurrentCache<SingleThreadTTLCache<K, V>>;

        pub fn new_ttl_cache<K, V>(maximum_size: usize, ttl: Duration) -> TTLLRUCache<K, V>
        where
            K: Hash + Eq + Clone + std::fmt::Debug,
            V: Clone,
        {
            TTLLRUCache::new(SingleThreadTTLCache::new(maximum_size, ttl))
        }
    }
}
