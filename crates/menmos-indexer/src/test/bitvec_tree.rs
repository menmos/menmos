use anyhow::Result;
use tempfile::TempDir;

use crate::bitvec_tree::BitvecTree;

#[test]
fn purging_out_of_range_idx() -> Result<()> {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();
    let tree = BitvecTree::new(&db, "test")?;

    tree.insert("some_tag", &0_u32.to_le_bytes())?;
    tree.insert("some_tag", &1_u32.to_le_bytes())?;
    tree.purge(4)?;

    Ok(())
}
