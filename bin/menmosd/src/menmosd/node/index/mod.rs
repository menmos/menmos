mod bitvec_tree;
mod documents;
mod meta;
mod root;
mod storage;

use bitvec_tree::BitvecTree;

pub use root::Index;

#[cfg(test)]
mod test;
