use std::borrow::Borrow;
use std::hash::Hash;

use tokio::sync::Mutex;

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

    pub async fn insert(&self, key: K, value: V) -> (bool, Option<V>) {
        let mut guard = self.cache.lock().await;
        guard.insert(key, value)
    }

    pub async fn batch_insert<I: Iterator<Item = (K, V)>>(&self, it: I) {
        let mut guard = self.cache.lock().await;
        for (k, v) in it {
            guard.insert(k, v);
        }
    }

    pub async fn get<Q: ?Sized>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let mut guard = self.cache.lock().await;
        guard.get(key).cloned()
    }

    pub async fn invalidate<Q: ?Sized>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let mut guard = self.cache.lock().await;
        guard.invalidate(key)
    }

    pub async fn clear(&self) {
        let mut guard = self.cache.lock().await;
        guard.clear()
    }
}
