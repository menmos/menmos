//! Useful prefabricated payloads.

use serde::{Deserialize, Serialize};

/// Standard message response.
///
/// Use when returning a single piece of unstructured information from your warp route. (e.g. "Upload Complete")
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MessageResponse {
    pub message: String,
}

impl MessageResponse {
    pub fn new<S: Into<String>>(message: S) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Standard error response.
///
/// Contains the error body serialized as a string.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ErrorResponse {
    pub error: String,
}
