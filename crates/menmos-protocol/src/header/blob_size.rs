use headers::{Error, Header, HeaderName, HeaderValue};

use once_cell::sync::Lazy;

static X_BLOB_SIZE: Lazy<HeaderName> = Lazy::new(|| HeaderName::from_static("x-blob-size"));

pub struct BlobSizeHeader(pub u64);

impl Header for BlobSizeHeader {
    fn name() -> &'static HeaderName {
        &X_BLOB_SIZE
    }

    fn decode<'i, I>(values: &mut I) -> std::result::Result<Self, Error>
    where
        Self: Sized,
        I: Iterator<Item = &'i HeaderValue>,
    {
        let first_value = values.next().ok_or_else(Error::invalid)?;
        let value_str = first_value.to_str().map_err(|_| Error::invalid())?;
        let size = value_str.parse::<u64>().map_err(|_| Error::invalid())?;

        if values.next().is_some() {
            tracing::trace!("x-blob-size doesn't support multiple assignment");
            return Err(Error::invalid());
        }

        Ok(Self(size))
    }

    fn encode<E: Extend<HeaderValue>>(&self, values: &mut E) {
        let value = HeaderValue::from_str(&format!("{}", self.0)).unwrap(); // Always safe
        values.extend(vec![value].into_iter())
    }
}
