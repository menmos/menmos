use nom::bytes::complete::{take_till, take_while, take_while1};
use nom::character::complete::char;
use nom::combinator::map;
use nom::sequence::{delimited, tuple};
use nom::IResult;

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

/// Eats {0-n} whitespace characters.
pub fn whitespace(i: &str) -> IResult<&str, &str> {
    let chars = " \t\r\n";
    take_while(move |c| chars.contains(c))(i)
}

pub fn identifier(i: &str) -> IResult<&str, String> {
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
