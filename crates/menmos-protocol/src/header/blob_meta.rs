use headers::{Error, Header, HeaderName, HeaderValue};

use interface::BlobMetaRequest;

use once_cell::sync::Lazy;

static X_BLOB_META: Lazy<HeaderName> = Lazy::new(|| HeaderName::from_static("x-blob-meta"));

pub struct BlobMetaHeader(pub BlobMetaRequest);

// God bless typed headers 😍
impl Header for BlobMetaHeader {
    fn name() -> &'static HeaderName {
        &X_BLOB_META
    }

    fn decode<'i, I>(values: &mut I) -> std::result::Result<Self, Error>
    where
        Self: Sized,
        I: Iterator<Item = &'i HeaderValue>,
    {
        let first_value = values.next().ok_or_else(Error::invalid)?;
        let json_bytes = base64::decode(first_value.as_bytes()).map_err(|e| {
            tracing::debug!("failed to b64 decode blob meta: {e}");
            Error::invalid()
        })?;
        let meta: BlobMetaRequest = serde_json::from_slice(&json_bytes).map_err(|e| {
            tracing::debug!("failed to deserialize blob meta: {e}");
            Error::invalid()
        })?;

        if values.next().is_some() {
            tracing::debug!("x-blob-meta doesn't support multiple assignment");
            return Err(Error::invalid());
        }

        Ok(Self(meta))
    }

    fn encode<E: Extend<HeaderValue>>(&self, values: &mut E) {
        let serialized = serde_json::to_vec(&self.0).unwrap(); // BlobMetaRequest is always serializable.
        let encoded = base64::encode(&serialized);
        let value = HeaderValue::from_str(&encoded).unwrap(); // b64 is always a valid header value.

        // TODO: Replace the vec allocation with Extend::extend_one once it is stabilized
        values.extend(vec![value].into_iter())
    }
}
