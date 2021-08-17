use std::ops;

use serde::{Deserialize, Serialize};

use snafu::{ensure, Snafu};

use crate::{parser, Resolver, Span};

/// The error type returned by the query parser.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("parse error"))]
    ParseError { message: String },
}

/// A parsed (or manually constructed) query expression.
///
/// An expression can be arbitrarily nested.
#[derive(Clone, Debug, Deserialize, Hash, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum Expression {
    /// Tag expression.
    ///
    /// Evaluates to resolver items where the tag is present.
    Tag { tag: String },
    /// Key/Value expression.
    ///
    /// Evaluates to resolver items where the given key/value pair is present.
    KeyValue { key: String, value: String },
    /// HasKey expression.
    ///
    /// Evaluates to resolver items where the given key is present (in any key/value pair).
    HasKey { key: String },
    /// Parent expression.
    ///
    /// Evaluates to resolver items having the provided value as a parent.
    Parent { parent: String },
    /// And expression.
    ///
    /// Evaluates to the intersection of its two sub-expressions.
    And {
        and: (Box<Expression>, Box<Expression>),
    },
    /// Or expression.
    /// Evaluates to the union of its two sub-expressions.
    Or {
        or: (Box<Expression>, Box<Expression>),
    },
    /// Not expression.
    /// Evaluates to the negation of its sub-expression.
    Not { not: Box<Expression> },
    /// Empty expression.
    /// Evaluates to all items resolvable by the resolver.
    Empty,
}

impl Expression {
    /// Parse an expression from a string.
    ///
    /// Returns an error if the provided string could not be parsed.
    ///
    /// # Examples
    ///
    /// ```
    /// use rapidquery::Expression;
    ///
    /// let expression = "a && b";
    /// match Expression::parse(expression) {
    ///     Ok(expr) => println!("Got expression: {:?}", expr),
    ///     Err(e) => panic!("failed to parse")
    /// }
    /// ```
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

    /// Evaluate an expression using a resolver.
    ///
    /// The resolver is tasked with resolving the sets corresponding to the various sub-expressions kinds (tags, key/value pairs, etc.).
    /// From there, the evaluator will recursively compute the query, calling the resolver when appropriate.
    ///
    /// # Examples
    /// ```no_run
    /// use rapidquery::{Expression, Resolver};
    ///
    /// let resolver: Box<dyn Resolver<bool, Error = std::io::Error>> = {
    ///     unimplemented!()
    /// };
    ///
    /// let expr = Expression::Empty;;
    ///
    /// let evaluation: bool = expr.evaluate(&resolver)?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn evaluate<R, V, E>(&self, resolver: &R) -> Result<V, E>
    where
        V: ops::BitAndAssign + ops::BitOrAssign + ops::Not<Output = V> + Span + std::fmt::Display,
        R: Resolver<V, Error = E>,
    {
        match self {
            Expression::Empty => resolver.resolve_empty(),
            Expression::Tag { tag } => resolver.resolve_tag(tag),
            Expression::KeyValue { key, value } => resolver.resolve_key_value(key, value),
            Expression::HasKey { key } => resolver.resolve_key(key),
            Expression::Parent { parent } => resolver.resolve_children(parent),
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

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, convert::Infallible};

    use super::*;

    #[derive(Default)]
    struct MockResolver {
        tags: Vec<String>,
        kv: HashMap<String, String>,
        keys: Vec<String>,
        parents: Vec<String>,
    }

    impl MockResolver {
        pub fn with_tag<S: Into<String>>(mut self, tag: S) -> Self {
            self.tags.push(tag.into());
            self
        }

        pub fn with_key_value<K: Into<String>, V: Into<String>>(mut self, k: K, v: V) -> Self {
            let key: String = k.into();
            self.keys.push(key.clone());
            self.kv.insert(key, v.into());
            self
        }

        pub fn with_parent<S: Into<String>>(mut self, parent: S) -> Self {
            self.parents.push(parent.into());
            self
        }
    }

    impl Resolver<bool> for MockResolver {
        type Error = Infallible;

        fn resolve_children(&self, parent_id: &str) -> Result<bool, Self::Error> {
            Ok(self.parents.contains(&String::from(parent_id)))
        }

        fn resolve_empty(&self) -> Result<bool, Self::Error> {
            Ok(true)
        }

        fn resolve_key(&self, key: &str) -> Result<bool, Self::Error> {
            Ok(self.keys.contains(&String::from(key)))
        }

        fn resolve_key_value(&self, key: &str, value: &str) -> Result<bool, Self::Error> {
            Ok(self.kv.get(&String::from(key)) == Some(&String::from(value)))
        }

        fn resolve_tag(&self, tag: &str) -> Result<bool, Self::Error> {
            Ok(self.tags.contains(&String::from(tag)))
        }
    }

    #[test]
    fn eval_empty_query() {
        assert!(Expression::Empty
            .evaluate(&MockResolver::default())
            .unwrap())
    }

    #[test]
    fn eval_tag_query() {
        assert!(Expression::Tag {
            tag: "hello".into()
        }
        .evaluate(&MockResolver::default().with_tag("hello"))
        .unwrap());
    }

    #[test]
    fn eval_tag_nomatch() {
        assert!(!Expression::Tag {
            tag: "yayeet".into()
        }
        .evaluate(&MockResolver::default().with_tag("Hello"))
        .unwrap())
    }

    #[test]
    fn eval_kv_query() {
        assert!(Expression::KeyValue {
            key: "key".into(),
            value: "val".into()
        }
        .evaluate(&MockResolver::default().with_key_value("key", "val"))
        .unwrap())
    }

    #[test]
    fn eval_kv_nomatch() {
        assert!(!Expression::KeyValue {
            key: "key".into(),
            value: "val".into()
        }
        .evaluate(&MockResolver::default().with_key_value("key", "yayeet"))
        .unwrap())
    }

    #[test]
    fn eval_and() {
        assert!(Expression::parse("a && b")
            .unwrap()
            .evaluate(&MockResolver::default().with_tag("a").with_tag("b"))
            .unwrap())
    }

    #[test]
    fn eval_and_nomatch() {
        assert!(!Expression::parse("a && b")
            .unwrap()
            .evaluate(&MockResolver::default().with_tag("a").with_tag("c"))
            .unwrap())
    }

    #[test]
    fn eval_or() {
        assert!(Expression::parse("a || b")
            .unwrap()
            .evaluate(&MockResolver::default().with_tag("b"))
            .unwrap())
    }

    #[test]
    fn eval_or_nomatch() {
        assert!(!Expression::parse("a || b")
            .unwrap()
            .evaluate(&MockResolver::default().with_tag("c"))
            .unwrap())
    }

    #[test]
    fn eval_not() {
        assert!(Expression::parse("!a")
            .unwrap()
            .evaluate(&MockResolver::default().with_tag("b"))
            .unwrap())
    }

    #[test]
    fn eval_not_nomatch() {
        assert!(!Expression::parse("!a")
            .unwrap()
            .evaluate(&MockResolver::default().with_tag("a"))
            .unwrap())
    }

    #[test]
    fn eval_parent() {
        assert!(
            Expression::Parent { parent: "p".into() } // There's no query syntax for parent queries _yet_.
                .evaluate(&MockResolver::default().with_parent("p"))
                .unwrap()
        )
    }

    #[test]
    fn eval_parent_nomatch() {
        assert!(!Expression::Parent { parent: "p".into() }
            .evaluate(&MockResolver::default().with_parent("3"))
            .unwrap())
    }

    #[test]
    fn eval_and_or_nested() {
        assert!(Expression::parse("(a || b) && c")
            .unwrap()
            .evaluate(&MockResolver::default().with_tag("a").with_tag("c"))
            .unwrap())
    }

    #[test]
    fn eval_not_nested() {
        assert!(Expression::parse("!(a && b) && !(!c)")
            .unwrap()
            .evaluate(&MockResolver::default().with_tag("a").with_tag("c"))
            .unwrap())
    }
}
