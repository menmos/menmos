use std::sync::Arc;

use anyhow::Result;

use async_trait::async_trait;

#[async_trait]
pub trait Flush {
    async fn flush(&self) -> Result<()>;
}

pub trait IndexProvider {}
