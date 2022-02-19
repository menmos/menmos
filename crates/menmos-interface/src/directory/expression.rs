use serde::{Deserialize, Serialize};

use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::char;
use nom::combinator::map;
use nom::sequence::{preceded, separated_pair};
use nom::IResult;

use crate::FieldValue;
use rapidquery::parse::util::{identifier, string};

#[derive(Clone, Debug, Deserialize, Hash, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum ExpressionField {
    Tag { tag: String },
    Field { key: String, value: FieldValue },
    HasField { key: String },
}

impl ExpressionField {
    fn tag_node(i: &str) -> IResult<&str, Self> {
        map(alt((identifier, string)), |tag| ExpressionField::Tag {
            tag,
        })(i)
    }

    fn key_value_node(i: &str) -> IResult<&str, Self> {
        // TODO: Support parsing other field types here once we have them.
        map(
            separated_pair(identifier, tag("="), alt((identifier, string))),
            |(key, value)| ExpressionField::Field {
                key,
                value: FieldValue::Str(value),
            },
        )(i)
    }

    fn hasfield_node(i: &str) -> IResult<&str, Self> {
        map(preceded(char('@'), identifier), |key| {
            ExpressionField::HasField { key }
        })(i)
    }
}

impl rapidquery::Parse for ExpressionField {
    fn parse(i: &str) -> IResult<&str, Self> {
        alt((Self::hasfield_node, Self::key_value_node, Self::tag_node))(i)
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, convert::Infallible};

    use rapidquery::Expression;

    use super::*;

    #[test]
    fn parse_basic_tag() {
        let e = Expression::parse(" bing  ").unwrap();
        assert_eq!(
            e,
            Expression::Field(ExpressionField::Tag { tag: "bing".into() })
        );
    }

    #[test]
    fn parse_basic_hasfield() {
        let e = Expression::parse(" @type").unwrap();
        assert_eq!(
            e,
            Expression::Field(ExpressionField::HasField { key: "type".into() })
        )
    }

    #[test]
    fn parse_underscore_tag() {
        let e = Expression::parse(" bing_bong ").unwrap();
        assert_eq!(
            e,
            Expression::Field(ExpressionField::Tag {
                tag: "bing_bong".into()
            })
        );
    }

    #[test]
    fn parse_multiword_tag() {
        let e = Expression::parse(" \"hello world\"   ").unwrap();
        assert_eq!(
            e,
            Expression::Field(ExpressionField::Tag {
                tag: "hello world".into()
            })
        );
    }

    #[test]
    fn parse_basic_kv() {
        let e = Expression::parse("bing=bong").unwrap();
        assert_eq!(
            e,
            Expression::Field(ExpressionField::Field {
                key: "bing".into(),
                value: "bong".into(),
            })
        );
    }

    #[test]
    fn parse_multiword_kv() {
        let e = Expression::parse("bing = \"bada boom\"").unwrap();
        assert_eq!(
            e,
            Expression::Field(ExpressionField::Field {
                key: "bing".into(),
                value: "bada boom".into(),
            })
        );
    }

    #[test]
    fn parse_basic_and() {
        let e = Expression::parse("bing && type=image").unwrap();
        assert_eq!(
            e,
            Expression::And {
                and: (
                    Box::new(Expression::Field(ExpressionField::Tag {
                        tag: "bing".into()
                    })),
                    Box::new(Expression::Field(ExpressionField::Field {
                        key: "type".into(),
                        value: "image".into(),
                    }))
                )
            }
        );
    }

    #[test]
    fn chained_and() {
        let e = Expression::parse("hello && there && world").unwrap();

        let expected_expr = Expression::And {
            and: (
                Box::from(Expression::And {
                    and: (
                        Box::from(Expression::Field(ExpressionField::Tag {
                            tag: "hello".into(),
                        })),
                        Box::from(Expression::Field(ExpressionField::Tag {
                            tag: "there".into(),
                        })),
                    ),
                }),
                Box::from(Expression::Field(ExpressionField::Tag {
                    tag: "world".into(),
                })),
            ),
        };
        assert_eq!(e, expected_expr);
    }

    #[test]
    fn parse_basic_or() {
        let e = Expression::parse("type=image || bing").unwrap();
        assert_eq!(
            e,
            Expression::Or {
                or: (
                    Box::from(Expression::Field(ExpressionField::Field {
                        key: "type".into(),
                        value: "image".into(),
                    })),
                    Box::from(Expression::Field(ExpressionField::Tag {
                        tag: "bing".into()
                    }))
                )
            }
        )
    }

    #[test]
    fn parse_not_after_or() {
        let e = Expression::parse("to_b || !to_b").unwrap();
        assert_eq!(
            e,
            Expression::Or {
                or: (
                    Box::from(Expression::Field(ExpressionField::Tag {
                        tag: "to_b".into()
                    })),
                    Box::from(Expression::Not {
                        not: Box::from(Expression::Field(ExpressionField::Tag {
                            tag: "to_b".into()
                        }))
                    })
                )
            }
        )
    }

    #[test]
    fn parse_basic_not() {
        let e = Expression::parse("!type=image").unwrap();
        assert_eq!(
            e,
            Expression::Not {
                not: Box::from(Expression::Field(ExpressionField::Field {
                    key: "type".into(),
                    value: "image".into(),
                }))
            }
        );
    }

    #[test]
    fn parse_basic_subexpr() {
        let e = Expression::parse("a && (b || c)").unwrap();
        let expected_expression = Expression::And {
            and: (
                Box::from(Expression::Field(ExpressionField::Tag { tag: "a".into() })),
                Box::from(Expression::Or {
                    or: (
                        Box::from(Expression::Field(ExpressionField::Tag { tag: "b".into() })),
                        Box::from(Expression::Field(ExpressionField::Tag { tag: "c".into() })),
                    ),
                }),
            ),
        };

        assert_eq!(e, expected_expression);
    }

    #[derive(Default)]
    struct MockResolver {
        tags: Vec<String>,
        kv: HashMap<String, FieldValue>,
        keys: Vec<String>,
    }

    impl MockResolver {
        pub fn with_tag<S: Into<String>>(mut self, tag: S) -> Self {
            self.tags.push(tag.into());
            self
        }

        pub fn with_field<K: Into<String>, V: Into<FieldValue>>(mut self, k: K, v: V) -> Self {
            let key: String = k.into();
            self.keys.push(key.clone());
            self.kv.insert(key, v.into());
            self
        }
    }

    impl rapidquery::FieldResolver<bool> for MockResolver {
        type FieldType = ExpressionField;
        type Error = Infallible;

        fn resolve_empty(&self) -> Result<bool, Self::Error> {
            Ok(true)
        }

        fn resolve(&self, field: &Self::FieldType) -> Result<bool, Self::Error> {
            let val = match field {
                ExpressionField::HasField { key } => self.keys.contains(key),
                ExpressionField::Field { key, value } => self.kv.get(key) == Some(value),
                ExpressionField::Tag { tag } => self.tags.contains(tag),
            };

            Ok(val)
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
        assert!(Expression::Field(ExpressionField::Tag {
            tag: "hello".into()
        })
        .evaluate(&MockResolver::default().with_tag("hello"))
        .unwrap());
    }

    #[test]
    fn eval_tag_nomatch() {
        assert!(!Expression::Field(ExpressionField::Tag {
            tag: "yayeet".into()
        })
        .evaluate(&MockResolver::default().with_tag("Hello"))
        .unwrap())
    }

    #[test]
    fn eval_kv_query() {
        assert!(Expression::Field(ExpressionField::Field {
            key: "key".into(),
            value: "val".into(),
        })
        .evaluate(&MockResolver::default().with_field("key", "val"))
        .unwrap())
    }

    #[test]
    fn eval_kv_nomatch() {
        assert!(!Expression::Field(ExpressionField::Field {
            key: "key".into(),
            value: "val".into(),
        })
        .evaluate(&MockResolver::default().with_field("key", "yayeet"))
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
