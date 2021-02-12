use std::borrow::Borrow;
use std::hash::Hash;

use linked_hash_map::LinkedHashMap;

use crate::cache::EvictionPolicy;

struct Nothing;

pub struct LRUEvictionPolicy<K>
where
    K: Eq + Hash + Clone,
{
    data: LinkedHashMap<K, Nothing>,
}

impl<K> Default for LRUEvictionPolicy<K>
where
    K: Eq + Hash + Clone,
{
    fn default() -> Self {
        LRUEvictionPolicy {
            data: LinkedHashMap::new(),
        }
    }
}

impl<K> EvictionPolicy<K> for LRUEvictionPolicy<K>
where
    K: Eq + Hash + Clone,
{
    fn get_victim(&mut self) -> Option<K> {
        self.data.front().map(|(k, _)| k.clone())
    }

    fn on_eviction(&mut self, key: &K) {
        debug_assert!(self.data.front().unwrap().0 == key);
        self.data.pop_front();
    }
    fn on_insert(&mut self, key: &K) {
        self.data.insert(key.clone(), Nothing {});
    }
    fn on_update(&mut self, key: &K) {
        self.data.insert(key.clone(), Nothing {});
    }
    fn on_cache_hit<Q: ?Sized>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.data.get_refresh(key);
    }

    fn clear(&mut self) {
        self.data.clear();
    }

    fn invalidate<Q: ?Sized>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.data.remove(key);
    }
}
