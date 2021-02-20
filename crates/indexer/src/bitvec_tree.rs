use anyhow::Result;

use bitvec::prelude::*;

use byteorder::{LittleEndian, ReadBytesExt};

#[allow(clippy::clippy::unnecessary_wraps)]
fn concatenate_merge(
    _key: &[u8],              // the key being merged
    old_value: Option<&[u8]>, // the previous value, if one existed
    merged_bytes: &[u8],      // the new bytes being merged in
) -> Option<Vec<u8>> {
    let new_max_index = merged_bytes.as_ref().read_u32::<LittleEndian>().unwrap() as usize;

    let mut bv: BitVec = if let Some(v) = old_value {
        let mut b: BitVec = bincode::deserialize(v).unwrap();
        if b.len() <= (new_max_index + 1) {
            b.resize(new_max_index + 1, false);
        }
        b
    } else {
        bitvec![Lsb0, usize; 0; (new_max_index + 1) as usize]
    };

    unsafe {
        // Safe because we allocate the bitvector with a size above this index on the line above.
        *bv.get_unchecked_mut(new_max_index as usize) = true;
    }

    Some(bincode::serialize(&bv).unwrap())
}

pub struct BitvecTree {
    tree: sled::Tree,
}

impl BitvecTree {
    pub fn new(db: &sled::Db, name: &str) -> Result<Self> {
        let tree = db.open_tree(name)?;
        tree.set_merge_operator(concatenate_merge);

        Ok(Self { tree })
    }

    pub fn insert(&self, key: &str, serialized_idx: &[u8]) -> Result<()> {
        self.tree
            .merge(key.to_lowercase().as_bytes(), serialized_idx)?;
        Ok(())
    }

    pub fn load(&self, key: &str) -> Result<BitVec> {
        if let Some(ivec) = self.tree.get(key.to_lowercase().as_bytes())? {
            let bv: BitVec = bincode::deserialize_from(ivec.as_ref())?;
            Ok(bv)
        } else {
            Ok(BitVec::default())
        }
    }

    pub fn purge_key<K: AsRef<[u8]>>(&self, key: K, idx: u32) -> Result<()> {
        self.tree.update_and_fetch(key, |f| {
            let ivec = f.unwrap();
            let mut bv: BitVec = bincode::deserialize_from(ivec).unwrap();

            // It's possible we just loaded a bitvector that is too small for the index we're
            // trying to purge.
            // In that case, simply skip setting the index.
            if (idx as usize) < bv.len() {
                bv.set(idx as usize, false);
                let serialized_update = bincode::serialize(&bv).unwrap();
                Some(serialized_update)
            } else {
                // Don't update the bitvector.
                f.map(Vec::from)
            }
        })?;
        Ok(())
    }

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
        Ok(())
    }
}
