use branca::{errors::Error as BrancaError, Branca};

use snafu::{ResultExt, Snafu};

use crate::{TokenData, TOKEN_TTL_SECONDS};

#[derive(Debug, Snafu)]
pub enum SignatureError {
    TokenInitializationError { source: BrancaError },
    TokenGenerationError { source: BrancaError },

    SerializationError { source: bincode::Error },
}

type Result<T> = std::result::Result<T, SignatureError>;

pub fn sign<B: AsRef<str>, K: AsRef<str>>(for_blob: B, key: K) -> Result<String> {
    let mut token = Branca::new(key.as_ref().as_bytes()).context(TokenInitializationError)?;
    token
        .set_ttl(TOKEN_TTL_SECONDS)
        .set_timestamp(chrono::Utc::now().timestamp() as u32);

    let message = TokenData::new(String::from(for_blob.as_ref()));
    let encoded = bincode::serialize(&message).context(SerializationError)?;

    let str_token = token.encode(&encoded).context(TokenGenerationError)?;
    Ok(str_token)
}
