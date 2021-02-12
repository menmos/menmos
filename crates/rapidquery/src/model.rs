use std::ops;

use serde::{Deserialize, Serialize};

use snafu::{ensure, Snafu};

use crate::{parser, Resolver, Span};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("parse error"))]
    ParseError { message: String },
}

#[derive(Clone, Debug, Deserialize, Hash, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum Expression {
    Tag {
        tag: String,
    },
    KeyValue {
        key: String,
        value: String,
    },
    HasKey {
        key: String,
    },
    Parent {
        parent: String,
    },
    And {
        and: (Box<Expression>, Box<Expression>),
    },
    Or {
        or: (Box<Expression>, Box<Expression>),
    },
    Not {
        not: Box<Expression>,
    },
    Empty,
}

impl Expression {
    pub fn parse<S: AsRef<str>>(str_expr: S) -> Result<Self, Error> {
        let (rest, expr) =
            parser::expression(str_expr.as_ref()).map_err(|e| Error::ParseError {
                message: e.to_string(),
            })?;

        ensure!(
            rest.is_empty(),
            ParseError {
                message: "incomplete parse".to_string()
            }
        );

        Ok(expr)
    }

    pub fn evaluate<R, V, E>(&self, resolver: &R) -> Result<V, E>
    where
        V: ops::BitAndAssign + ops::BitOrAssign + ops::Not<Output = V> + Span + std::fmt::Display,
        R: Resolver<V, Error = E>,
    {
        match self {
            Expression::Empty => resolver.resolve_empty(),
            Expression::Tag { tag } => resolver.resolve_tag(&tag),
            Expression::KeyValue { key, value } => resolver.resolve_key_value(&key, &value),
            Expression::HasKey { key } => resolver.resolve_key(&key),
            Expression::Parent { parent } => resolver.resolve_children(&parent),
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
                let (mut biggest, smallest) = if lhs_bv.span() > rhs_bv.span() {
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
                let (mut biggest, smallest) = if lhs_bv.span() > rhs_bv.span() {
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

impl Default for Expression {
    fn default() -> Self {
        Self::Empty
    }
}
