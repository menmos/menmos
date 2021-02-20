use branca::Branca;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use warp::{filters::BoxedFilter, Filter};

use crate::reject;

const TOKEN_TTL_SECONDS: u32 = 60 * 60 * 6; // 6 hours.

pub fn make_token<K: AsRef<str>, D: Serialize>(key: K, data: D) -> anyhow::Result<String> {
    let mut token = Branca::new(key.as_ref().as_bytes())?;
    token
        .set_ttl(TOKEN_TTL_SECONDS)
        .set_timestamp(chrono::Utc::now().timestamp() as u32);

    let encoded_body = bincode::serialize(&data)?;
    Ok(token.encode(&encoded_body)?)
}

#[derive(Deserialize, Serialize)]
pub struct StorageNodeIdentity {
    pub id: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct UserIdentity {
    pub username: String,
    pub admin: bool,
    pub blobs_whitelist: Option<Vec<String>>, // If none, all blobs are allowed.
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Signature {
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

pub fn storage_node(
    key: String,
) -> impl Filter<Extract = (StorageNodeIdentity,), Error = warp::Rejection> + Clone {
    warp::header::optional::<String>("authorization")
        .and(warp::any().map(move || key.clone()))
        .and_then(validate_storage_node_token)
}
