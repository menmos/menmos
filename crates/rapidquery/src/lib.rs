//! Generic query parsing & evaluation library.
//!
//! RapidQuery is menmos' query evaluation engine.
//!
//! Queries are modeled (and parsed) as a tree of evaluable nodes.
//! To evaluate a query, the user provides a struct (called a `Resolver`) capable of resolving the query context.
//!
//! From there, RapidQuery will figure out how to evaluate the query, and will ultimately return the set of items matching the query.
mod expression;
mod interface;

#[cfg(feature = "parse")]
pub mod parse;
#[cfg(feature = "parse")]
pub use parse::Parse;

pub use expression::Expression;
pub use interface::{FieldResolver, Sizeable};
