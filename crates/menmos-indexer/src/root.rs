use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use async_trait::async_trait;

use crate::iface::{Flush, IndexProvider};

pub struct Index {
    db: sled::Db,
}

impl Index {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = sled::open(path.as_ref())?;

        Ok(Self { db })
    }
}

#[async_trait]
impl Flush for Index {
    async fn flush(&self) -> Result<()> {
        self.db.flush_async().await?;

        Ok(())
    }
}

impl IndexProvider for Index {}
