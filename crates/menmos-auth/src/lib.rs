//! Menmos authentication library
mod storage_node_identity;
mod user_identity;

pub use storage_node_identity::StorageNodeIdentity;
pub use user_identity::UserIdentity;

use std::fmt::Debug;

use branca::Branca;

use serde::{Deserialize, Serialize};

const TOKEN_TTL_SECONDS: u32 = 60 * 60 * 6; // 6 hours.

/// The encryption key format that is expected by menmos_auth.
///
/// For the axum handlers, menmos_auth gets this structure from an extension layer that must be
/// set manually in your axum router.
#[derive(Clone, PartialEq, Eq)]
pub struct EncryptionKey {
    pub key: String,
}

/// Generate a signed token from an encryption key and a serializable payload.
///
/// The generated token will be valid for six hours.
///
/// The encryption key *must* be exactly 32 characters long, else an error will be returned.
///
/// # Examples
/// ```
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct UserInfo {
///     username: String,
///     is_admin: bool
/// }
///
/// let encryption_key = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"; // 32 characters.
/// let token_data = UserInfo{username: "johnsmith".into(), is_admin: true};
///
/// let token = menmos_auth::make_token(encryption_key, &token_data)?;
/// # Ok::<(), anyhow::Error>(())
/// ```
pub fn make_token<K: AsRef<str>, D: Serialize>(key: K, data: D) -> anyhow::Result<String> {
    let mut token = Branca::new(key.as_ref().as_bytes())?;
    token
        .set_ttl(TOKEN_TTL_SECONDS)
        .set_timestamp(time::OffsetDateTime::now_utc().unix_timestamp() as u32);

    let encoded_body = bincode::serialize(&data)?;
    Ok(token.encode(&encoded_body)?)
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct Signature {
    pub signature: Option<String>,
}
