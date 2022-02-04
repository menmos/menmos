use std::borrow::Borrow;
use std::hash::Hash;

use linked_hash_map::LinkedHashMap;

use crate::cache::EvictionPolicy;

struct Nothing;

// TODO: This LRU implementation isn't great.
// Since caches aren't super used in menmos right now this is acceptable,
// but we might need to improve this in the future.
pub struct LruEvictionPolicy<K>
where
    K: Eq + Hash + Clone,
{
    data: LinkedHashMap<K, Nothing>,
}

impl<K> Default for LruEvictionPolicy<K>
where
    K: Eq + Hash + Clone,
{
    fn default() -> Self {
        LruEvictionPolicy {
            data: LinkedHashMap::new(),
        }
    }
}

impl<K> EvictionPolicy<K> for LruEvictionPolicy<K>
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

#[cfg(test)]
mod tests {
    use super::*;

    fn expect_victims<T: Copy + std::fmt::Debug + Eq + Hash>(
        policy: &mut LruEvictionPolicy<T>,
        expected_victims: Vec<T>,
    ) {
        let mut victims = Vec::with_capacity(expected_victims.len());
        while let Some(candidate) = policy.get_victim() {
            victims.push(candidate);
            policy.on_eviction(&candidate);
        }

        assert_eq!(victims, expected_victims);
    }

    #[test]
    fn insert_evict_doesnt_reorder_items() {
        let mut policy = LruEvictionPolicy::default();

        policy.on_insert(&"a");
        policy.on_insert(&"b");
        policy.on_insert(&"c");

        expect_victims(&mut policy, vec!["a", "b", "c"]);
    }

    #[test]
    fn no_op_reordering() {
        let mut policy = LruEvictionPolicy::default();

        policy.on_insert(&"a");
        policy.on_insert(&"b");
        policy.on_insert(&"c");

        policy.on_cache_hit(&"c");

        expect_victims(&mut policy, vec!["a", "b", "c"]);
    }

    #[test]
    fn eviction_reordering() {
        let mut policy = LruEvictionPolicy::default();

        policy.on_insert(&"a");
        policy.on_insert(&"b");
        policy.on_insert(&"c");

        policy.on_cache_hit(&"b");

        expect_victims(&mut policy, vec!["a", "c", "b"]);
    }
}
