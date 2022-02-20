use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{anyhow, Result};
use interface::{BlobInfo, TaggedBlobInfo};

pub struct Index {
    db: sled::Db,
    size: AtomicU64,
}

impl Index {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = sled::open(path.as_ref())?;
        let size = db
            .iter()
            .filter_map(|result| match result {
                Ok((_key_ivec, value_ivec)) => {
                    match bincode::deserialize::<TaggedBlobInfo>(value_ivec.as_ref()) {
                        Ok(e) => Some(e.meta.size),
                        Err(e) => {
                            tracing::warn!(
                                "failed to deserialize blob during size computation: {}",
                                e
                            );
                            None
                        }
                    }
                }
                Err(_) => None,
            })
            .sum();

        Ok(Index {
            db,
            size: AtomicU64::new(size),
        })
    }

    pub fn get(&self, blob_id: &str) -> Result<Option<BlobInfo>> {
        self.db
            .get(blob_id.as_bytes())?
            .map(|blob_info_iv| {
                let tagged_info: TaggedBlobInfo =
                    bincode::deserialize(blob_info_iv.as_ref()).map_err(|e| anyhow!("{}", e))?;
                Ok(tagged_info.into())
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
        let tagged_info = TaggedBlobInfo::from(info.clone());
        let size_diff = match self
            .db
            .insert(blob_id.as_bytes(), bincode::serialize(&tagged_info)?)?
        {
            Some(old_ivec) => {
                let old_info: TaggedBlobInfo = bincode::deserialize(&old_ivec)?;
                -(old_info.meta.size as i128) + info.meta.size as i128
            }
            None => info.meta.size as i128,
        };

        if size_diff >= 0 {
            self.size.fetch_add(size_diff as u64, Ordering::SeqCst);
        } else {
            self.size.fetch_sub((-size_diff) as u64, Ordering::SeqCst);
        }

        Ok(())
    }

    pub fn remove(&self, blob_id: &str) -> Result<()> {
        if let Some(ivec) = self.db.remove(blob_id.as_bytes())? {
            let tagged_info: TaggedBlobInfo = bincode::deserialize(&ivec)?;
            self.size.fetch_sub(tagged_info.meta.size, Ordering::SeqCst);
        }
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        self.db.flush_async().await?;
        Ok(())
    }

    pub fn size(&self) -> u64 {
        self.size.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use interface::{BlobInfo, BlobMeta};

    use super::Index;

    #[test]
    fn get_nonexistent_info_returns_none() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let idx = Index::new(dir.path())?;

        assert!(idx.get("bad_blob")?.is_none());

        Ok(())
    }

    #[test]
    fn insert_get_remove_loop() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let idx = Index::new(dir.path())?;

        let info = BlobInfo {
            meta: BlobMeta::new(),
            owner: String::from("hello"),
        };

        idx.insert("asdf", &info)?;

        assert_eq!(idx.get("asdf")?.unwrap(), info);

        idx.remove("asdf")?;

        assert!(idx.get("asdf")?.is_none());

        Ok(())
    }

    #[test]
    fn get_all_keys() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let idx = Index::new(dir.path())?;

        idx.insert(
            "a",
            &BlobInfo {
                meta: BlobMeta::new(),
                owner: String::from("hello"),
            },
        )?;
        idx.insert(
            "b",
            &BlobInfo {
                meta: BlobMeta::new(),
                owner: String::from("hello"),
            },
        )?;

        let keys = idx.get_all_keys();
        assert_eq!(keys.as_slice(), &[String::from("a"), String::from("b")]);

        Ok(())
    }
}
