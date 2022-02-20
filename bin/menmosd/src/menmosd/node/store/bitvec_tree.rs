use anyhow::Result;

use bitvec::prelude::*;

use byteorder::{LittleEndian, ReadBytesExt};

#[allow(clippy::unnecessary_wraps)]
fn concatenate_merge(
    _key: &[u8],              // the key being merged
    old_value: Option<&[u8]>, // the previous value, if one existed
    merged_bytes: &[u8],      // the new bytes being merged in
) -> Option<Vec<u8>> {
    let new_max_index = merged_bytes.as_ref().read_u32::<LittleEndian>().unwrap() as usize;
    tracing::trace!("new max index: {}", new_max_index);

    let mut bv: BitVec = if let Some(v) = old_value {
        let mut b: BitVec = bincode::deserialize(v).unwrap();
        if b.len() <= (new_max_index + 1) {
            tracing::trace!(
                "old bitvector length is {}, resizing to {}",
                b.len(),
                new_max_index + 1
            );
            b.resize(new_max_index + 1, false);
        }
        b
    } else {
        bitvec![usize, Lsb0; 0; (new_max_index + 1) as usize]
    };

    unsafe {
        // Safe because we allocate the bitvector with a size above this index on the line above.
        *bv.get_unchecked_mut(new_max_index as usize) = true;
    }
    tracing::trace!(index = new_max_index, "flipped bit to true");

    Some(bincode::serialize(&bv).unwrap())
}

pub struct BitvecTree {
    tree: sled::Tree,
    name: String,
}

impl BitvecTree {
    pub fn new(db: &sled::Db, name: &str) -> Result<Self> {
        let tree = db.open_tree(format!("{}-bv-tree", name))?;
        tree.set_merge_operator(concatenate_merge);

        Ok(Self {
            tree,
            name: String::from(name),
        })
    }

    #[tracing::instrument(level = "trace", skip(self, serialized_idx), fields(name = % self.name))]
    pub fn insert(&self, key: &str, serialized_idx: &[u8]) -> Result<()> {
        self.insert_bytes(key.to_lowercase().as_bytes(), serialized_idx)
    }

    pub fn insert_bytes<T: AsRef<[u8]>>(&self, key: T, serialized_idx: &[u8]) -> Result<()> {
        self.tree.merge(key.as_ref(), serialized_idx)?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self), fields(name = % self.name))]
    pub fn load(&self, key: &str) -> Result<BitVec> {
        self.load_bytes(key.to_lowercase().as_bytes())
    }

    pub fn load_bytes<T: AsRef<[u8]>>(&self, key: T) -> Result<BitVec> {
        if let Some(ivec) = self.tree.get(key.as_ref())? {
            let ivec_slice: &[u8] = ivec.as_ref();
            let bv: BitVec = bincode::deserialize(ivec_slice)?;
            tracing::trace!(count = bv.count_ones(), "loaded");
            Ok(bv)
        } else {
            Ok(BitVec::default())
        }
    }

    #[tracing::instrument(level = "trace", skip(self, key), fields(name = % self.name))]
    pub fn purge_key<K: AsRef<[u8]>>(&self, key: K, idx: u32) -> Result<()> {
        self.tree.update_and_fetch(key, |f| {
            if let Some(ivec) = f {
                let mut bv: BitVec = bincode::deserialize(ivec).unwrap();

                // It's possible we just loaded a bitvector that is too small for the index we're
                // trying to purge.
                // In that case, simply skip setting the index.
                if (idx as usize) < bv.len() {
                    bv.set(idx as usize, false);
                    if bv.count_ones() == 0 {
                        // Delete the bitvector.
                        None
                    } else {
                        // Return the updated bitvector.
                        let serialized_update = bincode::serialize(&bv).unwrap();
                        Some(serialized_update)
                    }
                } else {
                    // Don't update the bitvector.
                    f.map(Vec::from)
                }
            } else {
                // Some other thread might've come in before us and deleted it already
                None
            }
        })?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self), fields(name = % self.name))]
    pub fn purge(&self, idx: u32) -> Result<()> {
        for (k, _) in self.tree.iter().filter_map(|f| f.ok()) {
            self.purge_key(k, idx)?;
        }
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        self.tree.flush_async().await?;
        Ok(())
    }

    // Note: This is not amazing encapsulation, but it allows the caller (the meta map)
    // to perform some more advanced operations in a much more efficient manner.
    // (without needing to specialize this bitvec tree for each type of map
    // supported in the index).
    pub fn tree(&self) -> &sled::Tree {
        &self.tree
    }

    pub fn clear(&self) -> Result<()> {
        self.tree.clear()?;
        tracing::trace!(name = %self.name, "cleared tree");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bitvec::prelude::*;

    #[test]
    fn bitvec_serialization_loop() {
        let bv = bitvec![usize, Lsb0; 1; 10];

        let serialized = bincode::serialize(&bv).unwrap();
        let deserialized: BitVec = bincode::deserialize(&serialized).unwrap();

        assert_eq!(bv, deserialized);
    }
}
