use std::path::Path;

use anyhow::Result;

use async_trait::async_trait;

use crate::{documents::DocumentIDStore, meta::MetadataStore, storage::StorageDispatch};
use crate::{
    iface::{Flush, IndexProvider},
    users::UsersStore,
};

pub struct Index {
    db: sled::Db,

    documents: DocumentIDStore,
    meta: MetadataStore,
    storage: StorageDispatch,
    users: UsersStore,
}

impl Index {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = sled::open(path.as_ref())?;
        let documents = DocumentIDStore::new(&db)?;
        let meta = MetadataStore::new(&db)?;
        let storage = StorageDispatch::new(&db)?;
        let users = UsersStore::new(&db)?;

        Ok(Self {
            db,
            documents,
            meta,
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
        self.storage.flush().await?;
        self.users.flush().await?;

        Ok(())
    }
}

impl IndexProvider for Index {
    type DocumentProvider = DocumentIDStore;
    type MetadataProvider = MetadataStore;
    type StorageProvider = StorageDispatch;
    type UserProvider = UsersStore;

    fn documents(&self) -> &Self::DocumentProvider {
        &self.documents
    }

    fn meta(&self) -> &Self::MetadataProvider {
        &self.meta
    }

    fn storage(&self) -> &Self::StorageProvider {
        &self.storage
    }

    fn users(&self) -> &Self::UserProvider {
        &self.users
    }
}
