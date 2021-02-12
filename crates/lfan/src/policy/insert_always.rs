use std::borrow::Borrow;
use std::hash::Hash;

use crate::cache::InsertionPolicy;

pub struct AlwaysInsertPolicy {}

impl Default for AlwaysInsertPolicy {
    fn default() -> Self {
        AlwaysInsertPolicy {}
    }
}

impl<K> InsertionPolicy<K> for AlwaysInsertPolicy {
    fn should_add(&mut self, _key: &K) -> bool {
        true
    }

    fn should_replace(&mut self, _candidate: &K, _victim: &K) -> bool {
        true
    }

    fn on_cache_hit<Q: ?Sized>(&mut self, _key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
    }
    fn on_cache_miss<Q: ?Sized>(&mut self, _key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
    }

    fn clear(&mut self) {}

    fn invalidate<Q: ?Sized>(&mut self, _key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
    }
}
