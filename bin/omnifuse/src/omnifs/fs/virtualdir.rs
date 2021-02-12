use client::Query;

use crate::config::Contents;
#[derive(Clone, Debug)]
pub enum VirtualDirectory {
    InMemory(Vec<String>),
    Query { query: Query },
    Mount { contents: Contents },
}
