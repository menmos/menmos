use std::fmt::Debug;

use apikit::reject::HTTPError;

use axum::extract::{Extension, FromRequest, Query, RequestParts, TypedHeader};
use axum::headers;
use axum::headers::authorization::{Authorization, Bearer};

use branca::Branca;

use serde::{Deserialize, Serialize};

use crate::{EncryptionKey, Signature, TOKEN_TTL_SECONDS};

/// Represents a storage node identity.
///
/// This is the body of contained in tokens used by storage nodes when they call the directory.
#[derive(Deserialize, Serialize)]
pub struct StorageNodeIdentity {
    pub id: String,
}

#[async_trait::async_trait]
impl<B: Send> FromRequest<B> for StorageNodeIdentity {
    type Rejection = HTTPError;

    async fn from_request(req: &mut RequestParts<B>) -> Result<Self, Self::Rejection> {
        let tok_maybe = if let Ok(TypedHeader(headers::Authorization(bearer))) =
            TypedHeader::<Authorization<Bearer>>::from_request(req).await
        {
            tracing::trace!("got a bearer token from authorization header");
            Some(bearer.token().to_string())
        } else if let Ok(Query(q)) = Query::<Signature>::from_request(req).await {
            if q.signature.is_some() {
                tracing::trace!("got a signature from query params");
            }
            q.signature
        } else {
            None
        };

        let token = tok_maybe.ok_or_else(|| {
            tracing::debug!("no token found");
            HTTPError::Forbidden
        })?;

        let Extension(EncryptionKey { key }) = Extension::<EncryptionKey>::from_request(req)
            .await
            .map_err(|e| {
                tracing::warn!("no encryption key in extension layer: {}", e);
                HTTPError::Forbidden
            })?;

        let token_decoder = Branca::new(key.as_bytes()).map_err(|e| {
            tracing::warn!("invalid encryption key: {}", e);
            HTTPError::Forbidden
        })?;
        let decoded = token_decoder
            .decode(&token, TOKEN_TTL_SECONDS)
            .map_err(|e| {
                tracing::warn!("invalid branca token: {}", e);
                HTTPError::Forbidden
            })?;

        Ok(bincode::deserialize(&decoded).map_err(|e| {
            tracing::debug!("token deserialize error: {}", e);
            HTTPError::Forbidden
        })?)
    }
}

impl Debug for StorageNodeIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "storage({})", &self.id)
    }
}
