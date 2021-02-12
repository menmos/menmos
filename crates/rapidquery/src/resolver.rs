use std::ops;

pub trait Resolver<V>
where
    V: ops::BitAndAssign + ops::BitOrAssign + ops::Not,
{
    type Error;

    fn resolve_tag(&self, tag: &str) -> Result<V, Self::Error>;
    fn resolve_key_value(&self, key: &str, value: &str) -> Result<V, Self::Error>;
    fn resolve_key(&self, key: &str) -> Result<V, Self::Error>;
    fn resolve_children(&self, parent_id: &str) -> Result<V, Self::Error>;
    fn resolve_empty(&self) -> Result<V, Self::Error>;
}
