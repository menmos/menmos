use std::ops::{BitAndAssign, BitOrAssign, Not};

pub trait FieldResolver<V>
where
    V: BitAndAssign + BitOrAssign + Not,
{
    type FieldType;
    type Error;

    fn resolve(&self, field: &Self::FieldType) -> Result<V, Self::Error>;
    fn resolve_empty(&self) -> Result<V, Self::Error>;
}

pub trait Sizeable {
    fn size(&self) -> usize;
}

#[cfg(feature = "bitvec_size")]
impl Sizeable for bitvec::prelude::BitVec {
    fn size(&self) -> usize {
        self.len()
    }
}
