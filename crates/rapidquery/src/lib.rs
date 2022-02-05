//! Generic query parsing & evaluation library.
//!
//! RapidQuery is menmos' query evaluation engine.
//!
//! Queries are modeled (and parsed) as a tree of evaluable nodes.
//! To evaluate a query, the user provides a struct (called a `Resolver`) capable of resolving the query context.
//!
//! From there, RapidQuery will figure out how to evaluate the query, and will ultimately return the set of items matching the query.
mod expression;
mod parser;

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

pub trait Parse: Sized {
    fn parse(input: &str) -> nom::IResult<&str, Self>;
}
