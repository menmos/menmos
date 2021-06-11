use anyhow::Result;

use async_trait::async_trait;

use super::iface::Flush;

pub trait StorageMappingStore: Flush {
    fn get_node_for_blob(&self, blob_id: &str) -> Result<Option<String>>;
    fn set_node_for_blob(&self, blob_id: &str, node_id: String) -> Result<()>;
    fn delete_blob(&self, blob_id: &str) -> Result<Option<String>>;
    fn clear(&self) -> Result<()>;
}

const DISPATCH_TREE: &str = "dispatch";
pub struct SledStorageMappingStore {
    tree: sled::Tree,
}

impl SledStorageMappingStore {
    pub fn new(db: &sled::Db) -> Result<Self> {
        let tree = db.open_tree(DISPATCH_TREE)?;
        Ok(Self { tree })
    }
}

#[async_trait]
impl Flush for SledStorageMappingStore {
    async fn flush(&self) -> Result<()> {
        self.tree.flush_async().await?;
        Ok(())
    }
}

impl StorageMappingStore for SledStorageMappingStore {
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
