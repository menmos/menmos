use std::borrow::Borrow;
use std::hash::Hash;

pub trait InsertionPolicy<K> {
    fn should_add(&mut self, key: &K) -> bool;
    fn should_replace(&mut self, candidate: &K, victim: &K) -> bool;

    fn on_cache_hit<Q: ?Sized>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq;

    fn on_cache_miss<Q: ?Sized>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq;

    fn clear(&mut self);

    fn invalidate<Q: ?Sized>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq;
}

pub trait EvictionPolicy<K> {
    fn get_victim(&mut self) -> Option<K>;

    fn on_eviction(&mut self, key: &K);
    fn on_insert(&mut self, key: &K);
    fn on_update(&mut self, key: &K);
    fn on_cache_hit<Q: ?Sized>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq;

    fn clear(&mut self);

    fn invalidate<Q: ?Sized>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq;
}
