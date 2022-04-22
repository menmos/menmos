use anyhow::Result;
use async_trait::async_trait;
use interface::{RoutingConfigState, TaggedRoutingConfigState};

use super::iface::Flush;

const ROUTING_KEY_MAP: &str = "routing_keys";

#[async_trait]
pub trait RoutingStore: Flush {
    fn get_routing_config(&self, username: &str) -> Result<Option<RoutingConfigState>>;
    fn set_routing_config(&self, username: &str, routing_key: &RoutingConfigState) -> Result<()>;
    fn delete_routing_config(&self, username: &str) -> Result<()>;
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
    #[tracing::instrument(name = "routing.get", level = "debug", skip(self))]
    fn get_routing_config(&self, username: &str) -> Result<Option<RoutingConfigState>> {
        let config_ivec_maybe =
            tokio::task::block_in_place(|| self.routing_keys.get(username.as_bytes()))?;

        if let Some(config_ivec) = config_ivec_maybe {
            let config: TaggedRoutingConfigState = bincode::deserialize(&config_ivec)?;
            Ok(Some(config.into()))
        } else {
            Ok(None)
        }
    }

    #[tracing::instrument(name = "routing.set", level = "debug", skip(self, routing_key))]
    fn set_routing_config(&self, username: &str, routing_key: &RoutingConfigState) -> Result<()> {
        let tagged_state = TaggedRoutingConfigState::from(routing_key.clone());
        let encoded = bincode::serialize(&tagged_state)?;

        tokio::task::block_in_place(|| {
            self.routing_keys
                .insert(username.as_bytes(), encoded.as_slice())
        })?;

        Ok(())
    }

    #[tracing::instrument(name = "routing.delete", level = "debug", skip(self))]
    fn delete_routing_config(&self, username: &str) -> Result<()> {
        tokio::task::block_in_place(|| self.routing_keys.remove(username.as_bytes()))?;
        Ok(())
    }
}
