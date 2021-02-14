use anyhow::Result;
use indexer::Index;

mod node_impl;

pub use node_impl::Directory;

use crate::Config;

pub fn make_node(c: &Config) -> Result<Directory<Index>> {
    let index = Index::new(&c.node.db_path)?;
    let node = Directory::new(index);
    Ok(node)
}

#[cfg(test)]
mod test;
