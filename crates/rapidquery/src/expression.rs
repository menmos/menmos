use std::ops::{BitAndAssign, BitOrAssign, Not};

use serde::{Deserialize, Serialize};

use crate::{FieldResolver, Sizeable};

#[derive(Debug, Clone, Deserialize, Hash, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum Expression<Field> {
    Field(Field),
    And { and: (Box<Self>, Box<Self>) },
    Or { or: (Box<Self>, Box<Self>) },
    Not { not: Box<Self> },
    Empty,
}

impl<Field> Default for Expression<Field> {
    fn default() -> Self {
        Expression::Empty
    }
}

impl<Field> Expression<Field> {
    pub fn evaluate<R, V, E>(&self, resolver: &R) -> Result<V, E>
    where
        R: FieldResolver<V, FieldType = Field, Error = E>,
        V: BitAndAssign + BitOrAssign + Not<Output = V> + Sizeable,
    {
        match self {
            Expression::Empty => resolver.resolve_empty(),
            Expression::Field(f) => resolver.resolve(f),
            Expression::Not { not } => {
                let mut all_bv = resolver.resolve_empty()?;
                all_bv &= not.evaluate(resolver)?;
                let mut negated = !all_bv;
                negated &= resolver.resolve_empty()?;
                Ok(negated)
            }
            Expression::And { and } => {
                let (lhs, rhs) = and;
                let lhs_bv = lhs.evaluate(resolver)?;
                let rhs_bv = rhs.evaluate(resolver)?;
                let (mut biggest, smallest) = if lhs_bv.size() > rhs_bv.size() {
                    (lhs_bv, rhs_bv)
                } else {
                    (rhs_bv, lhs_bv)
                };

                biggest &= smallest;
                Ok(biggest)
            }
            Expression::Or { or } => {
                let (lhs, rhs) = or;
                let lhs_bv = lhs.evaluate(resolver)?;
                let rhs_bv = rhs.evaluate(resolver)?;
                let (mut biggest, smallest) = if lhs_bv.size() > rhs_bv.size() {
                    (lhs_bv, rhs_bv)
                } else {
                    (rhs_bv, lhs_bv)
                };
                biggest |= smallest;
                Ok(biggest)
            }
        }
    }
}
