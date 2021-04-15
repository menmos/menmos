use anyhow::{anyhow, Result};

use async_trait::async_trait;
use interface::RoutingConfigState;

use crate::iface::{DynIter, Flush, RoutingMapper};

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
    fn get_routing_config(&self, username: &str) -> Result<Option<RoutingConfigState>> {
        if let Some(config_ivec) = self.routing_keys.get(username.as_bytes())? {
            let config: RoutingConfigState = bincode::deserialize(&config_ivec)?;
            Ok(Some(config))
        } else {
            Ok(None)
        }
    }

    fn set_routing_config(&self, username: &str, routing_key: &RoutingConfigState) -> Result<()> {
        let encoded = bincode::serialize(routing_key)?;
        self.routing_keys
            .insert(username.as_bytes(), encoded.as_slice())?;
        Ok(())
    }

    fn delete_routing_config(&self, username: &str) -> Result<()> {
        self.routing_keys.remove(username.as_bytes())?;
        Ok(())
    }

    fn iter(&self) -> DynIter<'static, Result<RoutingConfigState>> {
        DynIter::new(self.routing_keys.iter().map(|pair_result| {
            pair_result
                .map_err(|e| anyhow!(e))
                .and_then(|(_key_ivec, config_ivec)| {
                    bincode::deserialize(&config_ivec).map_err(|e| anyhow!(e))
                })
        }))
    }
}
