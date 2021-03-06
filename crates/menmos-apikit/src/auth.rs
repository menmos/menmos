//! Everything related to authentication.
use branca::Branca;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use warp::{filters::BoxedFilter, Filter};

use crate::reject;

const TOKEN_TTL_SECONDS: u32 = 60 * 60 * 6; // 6 hours.

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
/// let token = apikit::auth::make_token(encryption_key, &token_data)?;
/// # Ok::<(), anyhow::Error>(())
/// ```
pub fn make_token<K: AsRef<str>, D: Serialize>(key: K, data: D) -> anyhow::Result<String> {
    let mut token = Branca::new(key.as_ref().as_bytes())?;
    token
        .set_ttl(TOKEN_TTL_SECONDS)
        .set_timestamp(chrono::Utc::now().timestamp() as u32);

    let encoded_body = bincode::serialize(&data)?;
    Ok(token.encode(&encoded_body)?)
}

/// Represents a storage node identity.
///
/// This is the body of contained in tokens used by storage nodes when they call the directory.
#[derive(Deserialize, Serialize)]
pub struct StorageNodeIdentity {
    pub id: String,
}

/// Represents a user identity.
///
/// This is the body of user tokens.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct UserIdentity {
    pub username: String,
    pub admin: bool,
    pub blobs_whitelist: Option<Vec<String>>, // If none, all blobs are allowed.
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct Signature {
    pub signature: Option<String>,
}

fn optq<T: 'static + Default + Send + DeserializeOwned>() -> BoxedFilter<(T,)> {
    warp::any()
        .and(warp::query().or(warp::any().map(T::default)))
        .unify()
        .boxed()
}

fn extract_token<T, K>(key: K, token: &str) -> Result<T, warp::Rejection>
where
    T: DeserializeOwned,
    K: AsRef<str>,
{
    let token_decoder = Branca::new(key.as_ref().as_bytes()).map_err(|_| reject::Forbidden)?;

    let decoded = token_decoder
        .decode(token, TOKEN_TTL_SECONDS)
        .map_err(|_| reject::Forbidden)?;

    Ok(bincode::deserialize(&decoded).map_err(|_| reject::Forbidden)?)
}

fn strip_bearer(tok: &str) -> Result<&str, warp::Rejection> {
    const BEARER: &str = "Bearer ";

    if !tok.starts_with(BEARER) {
        log::debug!("invalid token");
        return Err(reject::Forbidden.into());
    }

    Ok(tok.trim_start_matches(BEARER))
}

async fn validate_user_tokens(
    header_token: Option<String>,
    url_signature_token: Option<String>,
    key: String,
) -> Result<UserIdentity, warp::Rejection> {
    let token = header_token
        .map(|t| strip_bearer(&t).map(String::from).ok())
        .flatten()
        .or(url_signature_token)
        .ok_or(reject::Forbidden)?;

    extract_token(&key, &token)
}

/// Warp filter to extract a user identity from the request. Use this when a route should be user-accessible.
///
/// This filter first looks for a bearer token in the `Authorization` header. Failing to find it,
/// it falls back on the `signature` query string parameter. The signature parameter is used by the system
/// to send pre-signed URLs to guests.
///
/// # Examples
/// ```
/// use apikit::auth::{user, UserIdentity};
/// use apikit::reply;
///
/// use warp::Filter;
///
/// let encryption_key = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"; // 32 characters.
///
/// let filter = warp::path("ping")
///                 .and(user(encryption_key.into()))
///                 .map(|user_identity: UserIdentity| reply::message(format!("Hello, {}", user_identity.username)));
/// ```
pub fn user(
    key: String,
) -> impl Filter<Extract = (UserIdentity,), Error = warp::Rejection> + Clone {
    warp::header::optional::<String>("authorization")
        .and(optq::<Signature>().map(|s: Signature| s.signature))
        .and(warp::any().map(move || key.clone()))
        .and_then(validate_user_tokens)
}

async fn validate_storage_node_token(
    token: Option<String>,
    key: String,
) -> Result<StorageNodeIdentity, warp::Rejection> {
    let token = token.ok_or(reject::Forbidden)?;
    let token = strip_bearer(&token)?;
    extract_token(&key, token)
}

/// Warp filter to extract a storage node identity from the request.
///
/// Use this when a route should be storage node-accessible.
///
/// This filter only looks for an bearer token in the `Authorization` header, because pre-signed URLs are not supported
/// for storage node calls.
///
/// # Examples
/// ```
/// use apikit::auth::{storage_node, StorageNodeIdentity};
/// use apikit::reply;
///
/// use warp::Filter;
///
/// let encryption_key = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"; // 32 characters.
///
/// let filter = warp::path("ping")
///                 .and(storage_node(encryption_key.into()))
///                 .map(|storage_identity: StorageNodeIdentity| {
///                     reply::message(format!("Storage node name: {}", storage_identity.id))
///                 });
/// ```
pub fn storage_node(
    key: String,
) -> impl Filter<Extract = (StorageNodeIdentity,), Error = warp::Rejection> + Clone {
    warp::header::optional::<String>("authorization")
        .and(warp::any().map(move || key.clone()))
        .and_then(validate_storage_node_token)
}
