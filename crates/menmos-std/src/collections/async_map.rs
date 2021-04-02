use std::hash::Hash;
use std::{borrow::Borrow, collections::HashMap};

use tokio::sync::RwLock;

#[derive(Debug, Default)]
pub struct AsyncHashMap<K, V>
where
    K: Eq + Hash,
    V: Clone,
{
    data: RwLock<HashMap<K, V>>,
}

impl<K, V> AsyncHashMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub fn new() -> Self {
        Self {
            data: Default::default(),
        }
    }

    pub async fn reserve(&self, additional: usize) {
        let mut guard = self.data.write().await;
        guard.reserve(additional);
    }

    pub async fn len(&self) -> usize {
        let guard = self.data.read().await;
        guard.len()
    }

    pub async fn is_empty(&self) -> bool {
        let guard = self.data.read().await;
        guard.is_empty()
    }

    pub async fn clear(&self) {
        let mut guard = self.data.write().await;
        guard.clear();
    }

    pub async fn get<Q: ?Sized>(&self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let guard = self.data.read().await;
        guard.get(k).cloned()
    }

    pub async fn get_all(&self) -> Vec<(K, V)> {
        let guard = self.data.read().await;
        let mut data = Vec::with_capacity(guard.len());

        for (key, value) in guard.iter() {
            data.push((key.clone(), value.clone()));
        }

        data
    }

    pub async fn contains_key<Q: ?Sized>(&self, k: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let guard = self.data.read().await;
        guard.contains_key(k)
    }

    pub async fn insert(&self, k: K, v: V) -> Option<V> {
        let mut guard = self.data.write().await;
        guard.insert(k, v)
    }

    pub async fn remove<Q: ?Sized>(&self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let mut guard = self.data.write().await;
        guard.remove(k)
    }
}
