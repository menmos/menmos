use branca::{errors::Error as BrancaError, Branca};

use snafu::{ensure, ResultExt, Snafu};

use crate::{TokenData, TOKEN_TTL_SECONDS};

#[derive(Debug, Snafu)]
pub enum ValidationError {
    TokenInitializationError { source: BrancaError },
    TokenDecodeError { source: BrancaError },
    DeserializationError { source: bincode::Error },

    Unauthorized,
}

type Result<T> = std::result::Result<T, ValidationError>;

pub fn validate<T: AsRef<str>, B: AsRef<str>, K: AsRef<str>>(
    token: T,
    blob_id: B,
    key: K,
) -> Result<()> {
    let token_decoder = Branca::new(key.as_ref().as_bytes()).context(TokenInitializationError)?;

    let decoded = token_decoder
        .decode(token.as_ref(), TOKEN_TTL_SECONDS)
        .context(TokenDecodeError)?;

    let data: TokenData = bincode::deserialize(&decoded).context(DeserializationError)?;

    ensure!(data.for_blob == blob_id.as_ref(), Unauthorized);

    Ok(())
}
