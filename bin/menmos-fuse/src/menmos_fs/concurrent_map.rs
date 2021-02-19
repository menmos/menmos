use std::collections::HashMap;
use std::hash::Hash;

use tokio::sync::Mutex;

#[derive(Default)]
pub struct ConcurrentMap<K, V>
where
    K: Eq + Hash,
    V: Clone,
{
    data: Mutex<HashMap<K, V>>,
}

impl<K, V> ConcurrentMap<K, V>
where
    K: Eq + Hash,
    V: Clone,
{
    pub fn new() -> Self {
        Self {
            data: Mutex::from(HashMap::new()),
        }
    }

    pub async fn insert(&self, key: K, value: V) {
        let mut guard = self.data.lock().await;
        (*guard).insert(key, value);
    }

    pub async fn get(&self, key: &K) -> Option<V> {
        let guard = self.data.lock().await;
        (*guard).get(key).cloned()
    }

    pub async fn remove(&self, key: &K) -> Option<V> {
        let mut guard = self.data.lock().await;
        (*guard).remove(key)
    }
}
