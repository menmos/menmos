use std::ops;

/// Trait describing a query context resolver.
///
/// The resolver is needed by RapidQuery to provide the initial values for computing the query.
///
/// For instance, when evaluating the query `(a || b) && c`, the evaluator will call `Resolver::load_tag()`
/// with `a`, `b`, and `c`. From there, it'll be in a position to compute the final result of the query.
pub trait Resolver<V>
where
    V: ops::BitAndAssign + ops::BitOrAssign + ops::Not,
{
    /// The error type returned by the resolver.
    type Error;

    /// Resolves the set matching a given tag.
    fn resolve_tag(&self, tag: &str) -> Result<V, Self::Error>;

    /// Resolves the set matching a given key/value pair.
    fn resolve_key_value(&self, key: &str, value: &str) -> Result<V, Self::Error>;

    /// Resolves the set matching any key/value pairs with the given key.
    fn resolve_key(&self, key: &str) -> Result<V, Self::Error>;

    /// Resolves the set matching a given parent ID.
    fn resolve_children(&self, parent_id: &str) -> Result<V, Self::Error>;

    /// Resolves the set of all possible items that could match a query.
    fn resolve_empty(&self) -> Result<V, Self::Error>;
}

impl<V, E> Resolver<V> for Box<dyn Resolver<V, Error = E>>
where
    V: ops::BitAndAssign + ops::BitOrAssign + ops::Not,
{
    type Error = E;

    fn resolve_tag(&self, tag: &str) -> Result<V, Self::Error> {
        (**self).resolve_tag(tag)
    }
    fn resolve_key_value(&self, key: &str, value: &str) -> Result<V, Self::Error> {
        (**self).resolve_key_value(key, value)
    }
    fn resolve_key(&self, key: &str) -> Result<V, Self::Error> {
        (**self).resolve_key(key)
    }
    fn resolve_children(&self, parent_id: &str) -> Result<V, Self::Error> {
        (**self).resolve_children(parent_id)
    }
    fn resolve_empty(&self) -> Result<V, Self::Error> {
        (**self).resolve_empty()
    }
}
