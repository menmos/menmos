use std::borrow::Borrow;
use std::collections::HashSet;
use std::hash::Hash;

use tokio::sync::RwLock;

pub struct AsyncSet<T>
where
    T: Eq + Hash,
{
    data: RwLock<HashSet<T>>,
}

impl<T> AsyncSet<T>
where
    T: Eq + Hash + Clone,
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

    pub async fn insert(&self, value: T) -> bool {
        let mut guard = self.data.write().await;
        guard.insert(value)
    }

    pub async fn contains<Q: ?Sized>(&self, value: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Hash + Eq,
    {
        let guard = self.data.read().await;
        guard.contains(value)
    }

    pub async fn remove<Q: ?Sized>(&self, value: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Hash + Eq,
    {
        let mut guard = self.data.write().await;
        guard.remove(value)
    }
}
