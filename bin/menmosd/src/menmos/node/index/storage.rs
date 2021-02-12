use std::collections::HashMap;
use std::sync::RwLock;

use anyhow::{anyhow, Result};

use async_trait::async_trait;

use chrono::{DateTime, Utc};

use interface::StorageNodeInfo;

use crate::node::iface::{Flush, StorageNodeMapper};

const DISPATCH_TREE: &str = "dispatch";
pub struct StorageDispatch {
    tree: sled::Tree,
    nodes: RwLock<HashMap<String, (StorageNodeInfo, DateTime<Utc>)>>,
}

impl StorageDispatch {
    pub fn new(db: &sled::Db) -> Result<Self> {
        let tree = db.open_tree(DISPATCH_TREE)?;
        Ok(Self {
            tree,
            nodes: Default::default(),
        })
    }
}

#[async_trait]
impl Flush for StorageDispatch {
    async fn flush(&self) -> Result<()> {
        self.tree.flush_async().await?;
        Ok(())
    }
}

impl StorageNodeMapper for StorageDispatch {
    fn get_node(&self, node_id: &str) -> Result<Option<(StorageNodeInfo, chrono::DateTime<Utc>)>> {
        let map_guard = self.nodes.write().map_err(|_| anyhow!("poisoned mutex"))?;
        let nodes = &*map_guard;
        match nodes.get(node_id) {
            Some((s, seen_at)) => Ok(Some((s.clone(), *seen_at))),
            None => Ok(None),
        }
    }

    fn get_all_nodes(&self) -> Result<Vec<StorageNodeInfo>> {
        let map_guard = self.nodes.write().map_err(|_| anyhow!("poisoned mutex"))?;
        let nodes = &*map_guard;

        Ok(nodes
            .iter()
            .map(|(_k, (node_info, _last_seen))| node_info.clone())
            .collect())
    }

    fn write_node(&self, info: StorageNodeInfo, seen_at: chrono::DateTime<Utc>) -> Result<bool> {
        let mut map_guard = self.nodes.write().map_err(|_| anyhow!("poisoned mutex"))?;
        let nodes = &mut *map_guard;
        let was_set = nodes.insert(info.id.clone(), (info, seen_at)).is_some();
        Ok(was_set)
    }

    fn delete_node(&self, node_id: &str) -> Result<()> {
        let mut map_guard = self.nodes.write().map_err(|_| anyhow!("poisoned mutex"))?;
        let nodes = &mut *map_guard;
        nodes.remove(node_id);
        Ok(())
    }

    fn get_node_for_blob(&self, blob_id: &str) -> Result<Option<String>> {
        Ok(self
            .tree
            .get(blob_id.as_bytes())?
            .map(|ivec| String::from_utf8_lossy(ivec.as_ref()).to_string()))
    }

    fn set_node_for_blob(&self, blob_id: &str, node_id: String) -> Result<()> {
        self.tree.insert(blob_id, node_id.as_bytes())?;
        Ok(())
    }

    fn delete_blob(&self, blob_id: &str) -> Result<Option<String>> {
        Ok(self
            .tree
            .remove(blob_id.as_bytes())?
            .map(|ivec| String::from_utf8_lossy(ivec.as_ref()).to_string()))
    }

    fn clear(&self) -> Result<()> {
        self.tree.clear()?;
        log::debug!("storage index destroyed");
        Ok(())
    }
}
