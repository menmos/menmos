#[derive(Clone, Debug, Deserialize, Hash, Serialize, PartialEq, Eq)]
pub enum ExpressionField {
    Tag(String),
    KeyValue((String, String)),
    Parent(String),
    HasKey(String),
}

impl ExpressionField {
    fn tag_node(i: &str) -> IResult<&str, Self> {
        map(alt((identifier, string)), ExpressionField::Tag)(i)
    }

    fn key_value_node(i: &str) -> IResult<&str, Self> {
        map(
            separated_pair(identifier, tag("="), alt((identifier, string))),
            |(key, value)| ExpressionField::KeyValue((key, value)),
        )(i)
    }

    fn haskey_node(i: &str) -> IResult<&str, Self> {
        map(preceded(char('@'), identifier), ExpressionField::HasKey)(i)
    }
}

impl rapidquery::Parse for ExpressionField {
    fn parse(i: &str) -> IResult<&str, Self> {
        alt((Self::tag_node, Self::key_value_node, Self::haskey_node))(i)
    }
}
