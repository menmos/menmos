use anyhow::Result;
use async_trait::async_trait;

use crate::node::store::iface::DynUserStore;

pub struct UserService {
    store: DynUserStore,
}

impl UserService {
    pub fn new(store: DynUserStore) -> Self {
        Self { store }
    }
}

#[async_trait]
impl interface::UserManagement for UserService {
    async fn login(&self, user: &str, password: &str) -> Result<bool> {
        self.store.authenticate(user, password)
    }

    async fn register(&self, user: &str, password: &str) -> Result<()> {
        self.store.add_user(user, password)
    }

    async fn has_user(&self, user: &str) -> Result<bool> {
        self.store.has_user(user)
    }

    async fn list(&self) -> Vec<String> {
        // TODO: This might need improving for cases where the number of users is very high.
        // It'd be premature to do now though.
        self.store.iter().filter_map(|f| f.ok()).collect()
    }

    async fn commit(&self) -> Result<()> {
        self.store.flush().await?;
        Ok(())
    }
}
