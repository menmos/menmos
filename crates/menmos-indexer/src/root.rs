use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use async_trait::async_trait;

use crate::{
    documents::DocumentIDStore, meta::MetadataStore, routing::RoutingStore,
    storage::StorageDispatch,
};
use crate::{
    iface::{Flush, IndexProvider},
    users::UsersStore,
};

pub struct Index {
    db: sled::Db,

    documents: Arc<DocumentIDStore>,
    meta: Arc<MetadataStore>,
    routing: Arc<RoutingStore>,
    storage: Arc<StorageDispatch>,
    users: Arc<UsersStore>,
}

impl Index {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = sled::open(path.as_ref())?;
        let documents = Arc::from(DocumentIDStore::new(&db)?);
        let meta = Arc::from(MetadataStore::new(&db)?);
        let routing = Arc::from(RoutingStore::new(&db)?);
        let storage = Arc::from(StorageDispatch::new(&db)?);
        let users = Arc::from(UsersStore::new(&db)?);

        Ok(Self {
            db,
            documents,
            meta,
            routing,
            storage,
            users,
        })
    }
}

#[async_trait]
impl Flush for Index {
    async fn flush(&self) -> Result<()> {
        self.db.flush_async().await?;
        self.documents.flush().await?;
        self.meta.flush().await?;
        self.routing.flush().await?;
        self.storage.flush().await?;
        self.users.flush().await?;

        Ok(())
    }
}

impl IndexProvider for Index {
    type DocumentProvider = DocumentIDStore;
    type MetadataProvider = MetadataStore;
    type RoutingProvider = RoutingStore;
    type StorageProvider = StorageDispatch;
    type UserProvider = UsersStore;

    fn documents(&self) -> Arc<Self::DocumentProvider> {
        self.documents.clone()
    }

    fn meta(&self) -> Arc<Self::MetadataProvider> {
        self.meta.clone()
    }

    fn routing(&self) -> Arc<Self::RoutingProvider> {
        self.routing.clone()
    }

    fn storage(&self) -> Arc<Self::StorageProvider> {
        self.storage.clone()
    }

    fn users(&self) -> Arc<Self::UserProvider> {
        self.users.clone()
    }
}
