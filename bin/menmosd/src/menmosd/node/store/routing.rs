use anyhow::{anyhow, Result};
use async_trait::async_trait;
use interface::RoutingConfigState;

use super::iface::Flush;
use super::DynIter;

const ROUTING_KEY_MAP: &str = "routing_keys";

#[async_trait]
pub trait RoutingStore: Flush {
    fn get_routing_config(&self, username: &str) -> Result<Option<RoutingConfigState>>;
    fn set_routing_config(&self, username: &str, routing_key: &RoutingConfigState) -> Result<()>;
    fn delete_routing_config(&self, username: &str) -> Result<()>;
    fn iter(&self) -> DynIter<'static, Result<RoutingConfigState>>;
}

pub struct SledRoutingStore {
    routing_keys: sled::Tree,
}

impl SledRoutingStore {
    pub fn new(db: &sled::Db) -> Result<Self> {
        let routing_keys = db.open_tree(ROUTING_KEY_MAP)?;
        Ok(Self { routing_keys })
    }
}

#[async_trait]
impl Flush for SledRoutingStore {
    async fn flush(&self) -> Result<()> {
        self.routing_keys.flush_async().await?;
        Ok(())
    }
}

impl RoutingStore for SledRoutingStore {
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
