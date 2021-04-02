use anyhow::Result;

use async_trait::async_trait;

use crate::iface::{Flush, RoutingMapper};

const ROUTING_KEY_MAP: &str = "routing_keys";

pub struct RoutingStore {
    routing_keys: sled::Tree,
}

impl RoutingStore {
    pub fn new(db: &sled::Db) -> Result<Self> {
        let routing_keys = db.open_tree(ROUTING_KEY_MAP)?;
        Ok(Self { routing_keys })
    }
}

#[async_trait]
impl Flush for RoutingStore {
    async fn flush(&self) -> Result<()> {
        self.routing_keys.flush_async().await?;
        Ok(())
    }
}

impl RoutingMapper for RoutingStore {
    fn get_routing_key(&self, username: &str) -> Result<Option<String>> {
        Ok(self
            .routing_keys
            .get(username.as_bytes())?
            .map(|key_ivec| String::from_utf8_lossy(key_ivec.as_ref()).to_string()))
    }

    fn set_routing_key(&self, username: &str, routing_key: &str) -> Result<()> {
        self.routing_keys
            .insert(username.as_bytes(), routing_key.as_bytes())?;
        Ok(())
    }

    fn delete_routing_key(&self, username: &str) -> Result<()> {
        self.routing_keys.remove(username.as_bytes())?;
        Ok(())
    }
}
