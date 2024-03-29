use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use time::OffsetDateTime;

/// Serialize an `OffsetDateTime` as its Unix timestamp
pub fn serialize<S: Serializer>(
    datetime: &OffsetDateTime,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    datetime.unix_timestamp_nanos().serialize(serializer)
}

/// Deserialize an `OffsetDateTime` from its Unix timestamp
pub fn deserialize<'a, D: Deserializer<'a>>(deserializer: D) -> Result<OffsetDateTime, D::Error> {
    let value = <_>::deserialize(deserializer)?;
    OffsetDateTime::from_unix_timestamp_nanos(value)
        .map_err(|err| de::Error::invalid_value(de::Unexpected::Other(&format!("{value}")), &err))
}
