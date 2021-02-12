use anyhow::Result;

mod iface;
mod index;
mod node_impl;

use index::Index;

pub use node_impl::Directory;

use crate::Config;

pub fn make_node(c: &Config) -> Result<Directory<Index>> {
    let index = Index::new(&c.node.db_path)?;
    let node = Directory::new(index);
    Ok(node)
}

#[cfg(test)]
mod test;
