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
        tokio::task::block_in_place(|| {
            Ok(self
                .tree
                .get(blob_id.as_bytes())?
                .map(|ivec| String::from_utf8(ivec.to_vec()).expect("node ID is not UTF-8")))
        })
    }

    fn set_node_for_blob(&self, blob_id: &str, node_id: String) -> Result<()> {
        tokio::task::block_in_place(|| {
            self.tree.insert(blob_id, node_id.as_bytes())?;
            Ok(())
        })
    }

    fn delete_blob(&self, blob_id: &str) -> Result<Option<String>> {
        tokio::task::block_in_place(|| {
            Ok(self
                .tree
                .remove(blob_id.as_bytes())?
                .map(|ivec| String::from_utf8(ivec.to_vec()).expect("node ID is not UTF-8")))
        })
    }

    fn clear(&self) -> Result<()> {
        tokio::task::block_in_place(|| self.tree.clear())?;
        tracing::debug!("storage index destroyed");
        Ok(())
    }
}
