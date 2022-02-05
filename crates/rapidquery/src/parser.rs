use std::convert::TryFrom;

use nom::{
    branch::alt,
    bytes::complete::{tag, take_till, take_while, take_while1},
    character::complete::char,
    combinator::{map, map_res},
    multi::many0,
    sequence::{delimited, preceded, separated_pair, tuple},
    IResult,
};

use snafu::{ensure, Snafu};
use tracing::error;

use crate::expression::Expression;
use crate::Parse;

#[derive(Debug, Snafu)]
pub enum ParserErr {
    InvalidOperator,
    ParseError { message: String },
}

enum Term<Field: Parse> {
    Field(Field),
    Not(Expression<Field>),
    SubExpr(Expression<Field>),
}

impl<Field: Parse> From<Term<Field>> for Expression<Field> {
    fn from(t: Term<Field>) -> Expression<Field> {
        match t {
            Term::Field(f) => Expression::Field { f },
            Term::Not(e) => Expression::Not { not: Box::from(e) },
            Term::SubExpr(e) => e,
        }
    }
}

enum TermOperator {
    And,
    Or,
}

impl TryFrom<String> for TermOperator {
    type Error = ParserErr;

    fn try_from(v: String) -> Result<TermOperator, ParserErr> {
        match v.as_ref() {
            "&&" => Ok(TermOperator::And),
            "||" => Ok(TermOperator::Or),
            _ => Err(ParserErr::InvalidOperator),
        }
    }
}

struct ParsedExpr<Field: Parse> {
    root: Term<Field>,
    trail: Vec<(TermOperator, Term<Field>)>,
}

impl<Field: Parse> From<ParsedExpr<Field>> for Expression<Field> {
    fn from(parsed: ParsedExpr<Field>) -> Expression<Field> {
        let mut root_expr = Expression::from(parsed.root);
        for (op, term) in parsed.trail {
            match op {
                TermOperator::And => {
                    root_expr = Expression::And {
                        and: (Box::from(root_expr), Box::from(Expression::from(term))),
                    }
                }
                TermOperator::Or => {
                    root_expr = Expression::Or {
                        or: (Box::from(root_expr), Box::from(Expression::from(term))),
                    }
                }
            }
        }
        root_expr
    }
}

/// Eats {0-n} whitespace characters.
fn whitespace(i: &str) -> IResult<&str, &str> {
    let chars = " \t\r\n";
    take_while(move |c| chars.contains(c))(i)
}

fn identifier(i: &str) -> IResult<&str, String> {
    map(
        delimited(
            whitespace,
            tuple((
                take_while1(move |c: char| c == '_' || c == '-' || c.is_alphabetic()),
                take_while(move |c: char| c == '_' || c == '.' || c.is_alphanumeric()),
            )),
            whitespace,
        ),
        |(a, b)| String::from(a) + b,
    )(i)
}

pub fn string(i: &str) -> IResult<&str, String> {
    map(
        delimited(
            whitespace,
            delimited(char('"'), take_till(|c| c == '"'), char('"')),
            whitespace,
        ),
        String::from,
    )(i)
}

fn subexpr_node<Field: Parse>(i: &str) -> IResult<&str, Term<Field>> {
    map(delimited(char('('), expression, char(')')), |e| {
        Term::SubExpr(e)
    })(i)
}

fn field<Field: Parse>(i: &str) -> IResult<&str, Term<Field>> {
    alt((map(Field::parse, |f| Term::Field(f)), subexpr_node))(i)
}

fn operator(i: &str) -> IResult<&str, TermOperator> {
    map_res(
        delimited(whitespace, alt((tag("&&"), tag("||"))), whitespace),
        |op_str| TermOperator::try_from(String::from(op_str)),
    )(i)
}

fn parsed_expr<Field: Parse>(i: &str) -> IResult<&str, ParsedExpr<Field>> {
    map(
        delimited(
            whitespace,
            tuple((field, many0(tuple((operator, field))))),
            whitespace,
        ),
        |(root, trail)| ParsedExpr { root, trail },
    )(i)
}

fn expression<Field: Parse>(i: &str) -> IResult<&str, Expression<Field>> {
    if i.is_empty() {
        Ok(("", Expression::default()))
    } else {
        let (rest, expr) = parsed_expr(i)?;
        Ok((rest, expr.into()))
    }
}

pub fn parse_expression<Field: Parse>(i: &str) -> Result<Expression<Field>, ParserErr> {
    let (r, expr) = expression(i).map_err(|e| ParserErr::ParseError {
        message: format!("{}", e.to_string()),
    })?;
    ensure!(
        !r.is_empty(),
        ParseSnafu {
            message: String::from("incomplete parse")
        }
    );

    Ok(expr)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_basic_tag() {
        let (rest, e) = expression(" bing  ").unwrap();
        assert!(rest.is_empty());
        assert_eq!(e, Expression::Tag { tag: "bing".into() });
    }

    #[test]
    fn parse_basic_haskey() {
        let (rest, e) = expression(" @type").unwrap();
        assert_eq!(rest, "");
        assert_eq!(e, Expression::HasKey { key: "type".into() });
    }

    #[test]
    fn parse_underscore_tag() {
        let (rest, e) = expression(" bing_bong ").unwrap();
        assert!(rest.is_empty());
        assert_eq!(
            e,
            Expression::Tag {
                tag: "bing_bong".into()
            }
        );
    }

    #[test]
    fn parse_multiword_tag() {
        let (rest, e) = expression(" \"hello world\"   ").unwrap();
        assert!(rest.is_empty());
        assert_eq!(
            e,
            Expression::Tag {
                tag: "hello world".into()
            }
        );
    }

    #[test]
    fn parse_basic_kv() {
        let (rest, e) = expression("bing=bong").unwrap();
        assert_eq!(rest, "");
        assert_eq!(
            e,
            Expression::KeyValue {
                key: "bing".into(),
                value: "bong".into(),
            }
        );
    }

    #[test]
    fn parse_multiword_kv() {
        let (rest, e) = expression("bing = \"bada boom\"").unwrap();
        assert!(rest.is_empty());
        assert_eq!(
            e,
            Expression::KeyValue {
                key: "bing".into(),
                value: "bada boom".into(),
            }
        );
    }

    #[test]
    fn parse_basic_and() {
        let (rest, e) = expression("bing && type=image").unwrap();
        assert!(rest.is_empty());
        assert_eq!(
            e,
            Expression::And {
                and: (
                    Box::from(Expression::Tag { tag: "bing".into() }),
                    Box::from(Expression::KeyValue {
                        key: "type".into(),
                        value: "image".into(),
                    })
                )
            }
        );
    }

    #[test]
    fn chained_and() {
        let (rest, e) = expression("hello && there && world").unwrap();
        assert_eq!(rest, "");

        let expected_expr = Expression::And {
            and: (
                Box::from(Expression::And {
                    and: (
                        Box::from(Expression::Tag {
                            tag: "hello".into(),
                        }),
                        Box::from(Expression::Tag {
                            tag: "there".into(),
                        }),
                    ),
                }),
                Box::from(Expression::Tag {
                    tag: "world".into(),
                }),
            ),
        };
        assert_eq!(e, expected_expr);
    }

    #[test]
    fn parse_basic_or() {
        let (rest, e) = expression("type=image || bing").unwrap();
        assert!(rest.is_empty());
        assert_eq!(
            e,
            Expression::Or {
                or: (
                    Box::from(Expression::KeyValue {
                        key: "type".into(),
                        value: "image".into(),
                    }),
                    Box::from(Expression::Tag { tag: "bing".into() })
                )
            }
        )
    }

    #[test]
    fn parse_not_after_or() {
        let (rest, e) = expression("to_b || !to_b").unwrap();
        assert_eq!(rest, "");
        assert_eq!(
            e,
            Expression::Or {
                or: (
                    Box::from(Expression::Tag { tag: "to_b".into() }),
                    Box::from(Expression::Not {
                        not: Box::from(Expression::Tag { tag: "to_b".into() })
                    })
                )
            }
        )
    }

    #[test]
    fn parse_basic_not() {
        let (rest, e) = expression("!type=image").unwrap();
        assert!(rest.is_empty());

        assert_eq!(
            e,
            Expression::Not {
                not: Box::from(Expression::KeyValue {
                    key: "type".into(),
                    value: "image".into(),
                })
            }
        );
    }

    #[test]
    fn parse_basic_subexpr() {
        let (rest, e) = expression("a && (b || c)").unwrap();
        assert_eq!(rest, "");

        let expected_expression = Expression::And {
            and: (
                Box::from(Expression::Tag { tag: "a".into() }),
                Box::from(Expression::Or {
                    or: (
                        Box::from(Expression::Tag { tag: "b".into() }),
                        Box::from(Expression::Tag { tag: "c".into() }),
                    ),
                }),
            ),
        };

        assert_eq!(e, expected_expression);
    }
}
