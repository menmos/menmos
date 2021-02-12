use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

use super::policy::{EvictionPolicy, InsertionPolicy};

#[derive(Default)]
pub struct Cache<K, V, IP, EP>
where
    K: Hash + Eq,
    IP: Default + InsertionPolicy<K>,
    EP: Default + EvictionPolicy<K>,
{
    data: HashMap<K, V>,

    insertion_policy: IP,
    eviction_policy: EP,

    maximum_size: usize,
}

impl<K, V, IP, EP> Cache<K, V, IP, EP>
where
    K: Hash + Eq + std::fmt::Debug,
    IP: Default + InsertionPolicy<K>,
    EP: Default + EvictionPolicy<K>,
{
    // TODO: Add a time-restricted cache.
    pub fn new(maximum_size: usize) -> Self {
        Self {
            data: HashMap::new(),

            insertion_policy: IP::default(),
            eviction_policy: EP::default(),

            maximum_size,
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> (bool, Option<V>) {
        let is_in_cache = self.data.contains_key(&key);

        if is_in_cache {
            // Update.
            self.eviction_policy.on_update(&key);
            self.data.insert(key, value);
            return (true, None);
        } else if self.data.len() < self.maximum_size {
            if self.insertion_policy.should_add(&key) {
                // Straight insert.
                self.eviction_policy.on_insert(&key);
                self.data.insert(key, value);
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
                self.data.insert(key, value);
                return (true, Some(evicted));
            }
        }
        (false, None)
    }

    pub fn get<Q: ?Sized>(&mut self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        match self.data.get(key) {
            Some(d) => {
                self.eviction_policy.on_cache_hit(key);
                self.insertion_policy.on_cache_hit(key);
                Some(d)
            }
            None => {
                self.insertion_policy.on_cache_miss(key);
                None
            }
        }
    }

    pub fn invalidate<Q: ?Sized>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.data.remove(key);
        self.insertion_policy.invalidate(key);
        self.eviction_policy.invalidate(key);
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }
}
