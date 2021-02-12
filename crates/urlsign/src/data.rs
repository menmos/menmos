use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct TokenData {
    pub for_blob: String,
}

impl TokenData {
    pub fn new(for_blob: String) -> Self {
        Self { for_blob }
    }
}
