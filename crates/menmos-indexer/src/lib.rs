mod bitvec_tree;
pub mod iface;
mod root;

use bitvec_tree::BitvecTree;

pub use root::Index;

#[cfg(test)]
mod test;
