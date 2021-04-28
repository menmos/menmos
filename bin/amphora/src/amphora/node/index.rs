use std::path::Path;

use anyhow::{anyhow, Result};
use interface::BlobInfo;

pub struct Index {
    db: sled::Db,
}

impl Index {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = sled::open(path.as_ref())?;
        Ok(Index { db })
    }

    pub fn get(&self, blob_id: &str) -> Result<Option<BlobInfo>> {
        self.db
            .get(blob_id.as_bytes())?
            .map(|blob_info_iv| {
                bincode::deserialize(blob_info_iv.as_ref()).map_err(|e| anyhow!("{}", e))
            })
            .transpose()
    }

    pub fn get_all_keys(&self) -> Vec<String> {
        self.db
            .iter()
            .filter_map(|r| r.ok())
            .map(|(k, _v)| String::from_utf8_lossy(k.as_ref()).to_string())
            .collect()
    }

    pub fn insert(&self, blob_id: &str, info: &BlobInfo) -> Result<()> {
        self.db
            .insert(blob_id.as_bytes(), bincode::serialize(&info)?)?;
        Ok(())
    }

    pub fn remove(&self, blob_id: &str) -> Result<()> {
        self.db.remove(blob_id.as_bytes())?;
        Ok(())
    }
}
