mod bitvec_tree;
mod documents;
mod id_map;
mod meta;
mod routing;
mod storage;
mod user;
mod util;

pub use util::DynIter;

pub mod iface {
    use anyhow::Result;
    use async_trait::async_trait;

    #[async_trait]
    pub trait Flush {
        async fn flush(&self) -> Result<()>;
    }

    pub use super::user::UserStore;
    pub type DynUserStore = Box<dyn UserStore + Send + Sync>;

    pub use super::routing::RoutingStore;
    pub type DynRoutingStore = Box<dyn RoutingStore + Send + Sync>;

    pub use super::documents::DocumentIdStore;
    pub type DynDocumentIDStore = Box<dyn DocumentIdStore + Send + Sync>;

    pub use super::meta::MetadataStore;
    pub type DynMetadataStore = Box<dyn MetadataStore + Send + Sync>;

    pub use super::storage::StorageMappingStore;
    pub type DynStorageMappingStore = Box<dyn StorageMappingStore + Send + Sync>;
}

pub mod sled {
    pub use super::documents::SledDocumentIdStore;
    pub use super::meta::SledMetadataStore;
    pub use super::routing::SledRoutingStore;
    pub use super::storage::SledStorageMappingStore;
    pub use super::user::SledUserStore;
}
