use std::borrow::Borrow;
use std::hash::Hash;

pub trait Cache {
    type Key: Hash + Eq + std::fmt::Debug;
    type Value;

    fn insert(&mut self, key: Self::Key, value: Self::Value) -> (bool, Option<Self::Value>);

    fn get<Q: ?Sized>(&mut self, key: &Q) -> Option<&Self::Value>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq;

    fn invalidate<Q: ?Sized>(&mut self, key: &Q)
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq;

    fn clear(&mut self);
}
