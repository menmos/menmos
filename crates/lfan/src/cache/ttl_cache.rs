use std::hash::Hash;
use std::{borrow::Borrow, time::Instant};
use std::{collections::HashMap, time::Duration};

use super::{
    policy::{EvictionPolicy, InsertionPolicy},
    Cache,
};

struct CacheItem<V> {
    item: V,
    last_seen: Instant,
}

impl<V> CacheItem<V> {
    pub fn is_expired(&self, ttl: &Duration) -> bool {
        let now = Instant::now();
        &now.duration_since(self.last_seen) > ttl
    }
}

impl<V> From<V> for CacheItem<V> {
    fn from(item: V) -> Self {
        CacheItem {
            item,
            last_seen: Instant::now(),
        }
    }
}

#[derive(Default)]
pub struct TTLCache<K, V, IP, EP>
where
    K: Hash + Eq,
    IP: Default + InsertionPolicy<K>,
    EP: Default + EvictionPolicy<K>,
{
    data: HashMap<K, CacheItem<V>>,

    ttl: Duration,

    insertion_policy: IP,
    eviction_policy: EP,

    maximum_size: usize,
}

impl<K, V, IP, EP> TTLCache<K, V, IP, EP>
where
    K: Hash + Eq + std::fmt::Debug,
    IP: Default + InsertionPolicy<K>,
    EP: Default + EvictionPolicy<K>,
{
    pub fn new(maximum_size: usize, ttl: Duration) -> Self {
        Self {
            data: HashMap::new(),
            ttl,
            insertion_policy: IP::default(),
            eviction_policy: EP::default(),
            maximum_size,
        }
    }
}

impl<K, V, IP, EP> Cache for TTLCache<K, V, IP, EP>
where
    K: Hash + Eq + std::fmt::Debug,
    IP: Default + InsertionPolicy<K>,
    EP: Default + EvictionPolicy<K>,
{
    type Key = K;
    type Value = V;

    fn insert(&mut self, key: K, value: V) -> (bool, Option<V>) {
        let is_in_cache = self.data.contains_key(&key);

        if is_in_cache {
            // Update.
            self.eviction_policy.on_update(&key);
            self.data.insert(key, value.into());
            return (true, None);
        } else if self.data.len() < self.maximum_size {
            if self.insertion_policy.should_add(&key) {
                // Straight insert.
                self.eviction_policy.on_insert(&key);
                self.data.insert(key, value.into());
                return (true, None);
            }
        } else {
            // Get a cache victim.
            let victim_key = self.eviction_policy.get_victim().unwrap(); // If this panics, there is most likely a bug in the cache.
            if self.insertion_policy.should_replace(&key, &victim_key) {
                // Evict the key
                self.eviction_policy.on_eviction(&victim_key);
                let evicted = self.data.remove(&victim_key).unwrap();

                self.eviction_policy.on_insert(&key);
                self.data.insert(key, value.into());
                return (true, Some(evicted.item));
            }
        }
        (false, None)
    }

    fn get<Q: ?Sized>(&mut self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let should_invalidate = if let Some(v) = self.data.get(key) {
            v.is_expired(&self.ttl)
        } else {
            false
        };

        if should_invalidate {
            self.invalidate(key);
        }

        match self.data.get(key) {
            Some(d) => {
                self.eviction_policy.on_cache_hit(key);
                self.insertion_policy.on_cache_hit(key);
                Some(&d.item)
            }
            None => {
                self.insertion_policy.on_cache_miss(key);
                None
            }
        }
    }

    fn invalidate<Q: ?Sized>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.data.remove(key);
        self.insertion_policy.invalidate(key);
        self.eviction_policy.invalidate(key);
    }

    fn clear(&mut self) {
        self.data.clear();
    }
}
