use std::borrow::Borrow;
use std::hash::Hash;

use tokio::sync::Mutex;

use crate::Cache;

use super::policy::{EvictionPolicy, InsertionPolicy};

#[derive(Default)]
pub struct ConcurrentCache<K, V, IP, EP>
where
    K: Hash + Eq,
    V: Clone,
    IP: Default + InsertionPolicy<K>,
    EP: Default + EvictionPolicy<K>,
{
    cache: Mutex<Cache<K, V, IP, EP>>,
}

impl<K, V, IP, EP> ConcurrentCache<K, V, IP, EP>
where
    K: Hash + Eq + std::fmt::Debug,
    V: Clone,
    IP: Default + InsertionPolicy<K>,
    EP: Default + EvictionPolicy<K>,
{
    pub fn new(maximum_size: usize) -> Self {
        Self {
            cache: Mutex::from(Cache::new(maximum_size)),
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
