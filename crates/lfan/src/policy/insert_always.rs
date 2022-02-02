use std::borrow::Borrow;
use std::hash::Hash;

use crate::cache::InsertionPolicy;

#[derive(Default)]
pub struct AlwaysInsertPolicy {}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn always_returns_true() {
        let mut policy = AlwaysInsertPolicy::default();

        for i in 0..10 {
            assert!(policy.should_add(&i))
        }
    }
}
