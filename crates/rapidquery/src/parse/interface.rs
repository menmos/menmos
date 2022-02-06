use crate::Expression;

use super::ParserErr;

pub trait Parse: Sized {
    fn parse(input: &str) -> nom::IResult<&str, Self>;
}

impl<Field: Parse> Expression<Field> {
    pub fn parse<S: AsRef<str>>(input: S) -> Result<Self, ParserErr> {
        super::parse_expression(input.as_ref())
    }
}
