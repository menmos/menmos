use std::hash::Hash;
use std::{borrow::Borrow, collections::HashMap};

use parking_lot::RwLock;

#[derive(Debug, Default)]
pub struct ConcurrentHashMap<K, V>
where
    K: Eq + Hash,
    V: Clone,
{
    data: RwLock<HashMap<K, V>>,
}

impl<K, V> ConcurrentHashMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub fn new() -> Self {
        Self {
            data: Default::default(),
        }
    }

    pub fn reserve(&self, additional: usize) {
        let mut guard = self.data.write();
        guard.reserve(additional);
    }

    pub fn len(&self) -> usize {
        let guard = self.data.read();
        guard.len()
    }

    pub fn is_empty(&self) -> bool {
        let guard = self.data.read();
        guard.is_empty()
    }

    pub fn clear(&self) {
        let mut guard = self.data.write();
        guard.clear();
    }

    pub fn get<Q: ?Sized>(&self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let guard = self.data.read();
        guard.get(k).cloned()
    }

    pub fn get_all(&self) -> Vec<(K, V)> {
        let guard = self.data.read();
        let mut data = Vec::with_capacity(guard.len());

        for (key, value) in guard.iter() {
            data.push((key.clone(), value.clone()));
        }

        data
    }

    pub fn contains_key<Q: ?Sized>(&self, k: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let guard = self.data.read();
        guard.contains_key(k)
    }

    pub fn insert(&self, k: K, v: V) -> Option<V> {
        let mut guard = self.data.write();
        guard.insert(k, v)
    }

    pub fn remove<Q: ?Sized>(&self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let mut guard = self.data.write();
        guard.remove(k)
    }
}
