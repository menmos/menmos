mod bitvec_tree;
mod documents;
pub mod iface;
mod meta;
mod root;
mod storage;
mod users;

use bitvec_tree::BitvecTree;

pub use root::Index;

#[cfg(test)]
mod test;
