use serde::{Deserialize, Serialize};

use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::char;
use nom::combinator::map;
use nom::sequence::{preceded, separated_pair};
use nom::IResult;

use rapidquery::parse::util::{identifier, string};

#[derive(Clone, Debug, Deserialize, Hash, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum ExpressionField {
    Tag { tag: String },
    KeyValue { key: String, value: String },
    Parent { parent: String },
    HasKey { key: String },
}

impl ExpressionField {
    fn tag_node(i: &str) -> IResult<&str, Self> {
        map(alt((identifier, string)), |tag| ExpressionField::Tag {
            tag,
        })(i)
    }

    fn key_value_node(i: &str) -> IResult<&str, Self> {
        map(
            separated_pair(identifier, tag("="), alt((identifier, string))),
            |(key, value)| ExpressionField::KeyValue { key, value },
        )(i)
    }

    fn haskey_node(i: &str) -> IResult<&str, Self> {
        map(preceded(char('@'), identifier), |key| {
            ExpressionField::HasKey { key }
        })(i)
    }
}

impl rapidquery::Parse for ExpressionField {
    fn parse(i: &str) -> IResult<&str, Self> {
        alt((Self::haskey_node, Self::key_value_node, Self::tag_node))(i)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use rapidquery::Expression;

    #[test]
    fn parse_basic_tag() {
        let e = Expression::parse(" bing  ").unwrap();
        assert_eq!(
            e,
            Expression::Field(ExpressionField::Tag { tag: "bing".into() })
        );
    }

    #[test]
    fn parse_basic_haskey() {
        let e = Expression::parse(" @type").unwrap();
        assert_eq!(
            e,
            Expression::Field(ExpressionField::HasKey { key: "type".into() })
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
            Expression::Field(ExpressionField::KeyValue {
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
            Expression::Field(ExpressionField::KeyValue {
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
                    Box::new(Expression::Field(ExpressionField::KeyValue {
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
                    Box::from(Expression::Field(ExpressionField::KeyValue {
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
                not: Box::from(Expression::Field(ExpressionField::KeyValue {
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
}
