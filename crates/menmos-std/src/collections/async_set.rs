use std::borrow::Borrow;
use std::collections::HashSet;
use std::hash::Hash;

use parking_lot::RwLock;

#[derive(Default)]
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

    pub fn insert(&self, value: T) -> bool {
        let mut guard = self.data.write();
        guard.insert(value)
    }

    pub fn contains<Q: ?Sized>(&self, value: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Hash + Eq,
    {
        let guard = self.data.read();
        guard.contains(value)
    }

    pub fn remove<Q: ?Sized>(&self, value: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Hash + Eq,
    {
        let mut guard = self.data.write();
        guard.remove(value)
    }
}
