use std::borrow::Borrow;
use std::hash::Hash;

use parking_lot::Mutex;

use super::Cache;

#[derive(Default)]
pub struct ConcurrentCache<C> {
    cache: Mutex<C>,
}

impl<C, K, V> ConcurrentCache<C>
where
    C: Cache<Key = K, Value = V>,
    K: Hash + Eq + std::fmt::Debug,
    V: Clone,
{
    pub fn new(cache: C) -> Self {
        Self {
            cache: Mutex::from(cache),
        }
    }

    pub fn insert(&self, key: K, value: V) -> (bool, Option<V>) {
        let mut guard = self.cache.lock();
        guard.insert(key, value)
    }

    pub fn batch_insert<I: Iterator<Item = (K, V)>>(&self, it: I) {
        let mut guard = self.cache.lock();
        for (k, v) in it {
            guard.insert(k, v);
        }
    }

    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let mut guard = self.cache.lock();
        guard.get(key).cloned()
    }

    pub fn invalidate<Q: ?Sized>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let mut guard = self.cache.lock();
        guard.invalidate(key)
    }

    pub fn clear(&self) {
        let mut guard = self.cache.lock();
        guard.clear()
    }
}
