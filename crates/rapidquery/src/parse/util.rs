use nom::bytes::complete::{tag, take_till, take_while, take_while1};
use nom::character::complete::{char, digit1};
use nom::combinator::{map, map_res, opt};
use nom::sequence::{delimited, tuple};
use nom::IResult;

/// Parses a quote-delimited string.
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

/// Parses an identifier.
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

pub fn integer(i: &str) -> IResult<&str, i64> {
    map_res(
        delimited(whitespace, tuple((opt(tag("-")), digit1)), whitespace),
        |(sign, rest)| match sign {
            Some(s) => (s.to_owned() + rest).parse::<i64>(),
            None => rest.parse::<i64>(),
        },
    )(i)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_string() {
        let (rest, out) = string("\"bing\"").unwrap();
        assert!(rest.is_empty());
        assert_eq!(out, String::from("bing"));
    }

    #[test]
    fn parse_string_spaces() {
        let (rest, out) = string("\"bing bong\"").unwrap();
        assert!(rest.is_empty());
        assert_eq!(out, String::from("bing bong"));
    }

    #[test]
    fn parse_string_unclosed_quotes() {
        assert!(string("\"bing").is_err());
    }

    #[test]
    fn parse_int() {
        let (rest, out) = integer("12345").unwrap();
        assert!(rest.is_empty());
        assert_eq!(out, 12345);
    }

    #[test]
    fn parse_int_negative() {
        let (rest, out) = integer("-12345").unwrap();
        assert!(rest.is_empty());
        assert_eq!(out, -12345);
    }

    #[test]
    fn parse_int_float() {
        let (rest, out) = integer("-12345.3").unwrap();
        assert_eq!(rest, ".3");
        assert_eq!(out, -12345);
    }
}
