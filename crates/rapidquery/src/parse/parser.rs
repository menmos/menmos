use std::convert::TryFrom;

use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::char,
    combinator::{map, map_res},
    multi::many0,
    sequence::{delimited, preceded, tuple},
    IResult,
};

use snafu::{ensure, Snafu};

use crate::Expression;

use super::util::whitespace;
use super::Parse;

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
            Term::Field(f) => Expression::Field(f),
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

fn subexpr_node<Field: Parse>(i: &str) -> IResult<&str, Term<Field>> {
    map(delimited(char('('), expression, char(')')), |e| {
        Term::SubExpr(e)
    })(i)
}

fn not_node<Field: Parse>(i: &str) -> IResult<&str, Term<Field>> {
    map(preceded(char('!'), field), |expr| Term::Not(expr.into()))(i)
}

fn field<Field: Parse>(i: &str) -> IResult<&str, Term<Field>> {
    alt((
        subexpr_node,
        not_node,
        map(Field::parse, |f| Term::Field(f)),
    ))(i)
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
        message: e.to_string(),
    })?;
    ensure!(
        r.is_empty(),
        ParseSnafu {
            message: format!("incomplete parse: [{}]", r)
        }
    );

    Ok(expr)
}
