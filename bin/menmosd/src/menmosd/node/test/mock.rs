use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::Result;

use async_trait::async_trait;

use crate::node::store::DynIter;
use crate::node::{
    service::UserService,
    store::iface::{Flush, UserStore},
};

#[derive(Default)]
struct MockUserStore {
    users: Mutex<HashMap<String, String>>,
}

#[async_trait]
impl Flush for MockUserStore {
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

impl UserStore for MockUserStore {
    fn authenticate(&self, username: &str, password: &str) -> Result<bool> {
        let guard = self.users.lock().unwrap();
        Ok(guard.get(username).cloned().unwrap_or(String::default()) == password)
    }

    fn add_user(&self, username: &str, password: &str) -> Result<()> {
        let mut guard = self.users.lock().unwrap();
        guard.insert(username.to_string(), password.to_string());
        Ok(())
    }

    fn has_user(&self, username: &str) -> Result<bool> {
        let guard = self.users.lock().unwrap();
        Ok(guard.contains_key(username))
    }

    fn iter(&self) -> DynIter<'static, Result<String>> {
        // Returning an iterator on something protected by a mutex = cursed.
        let guard = self.users.lock().unwrap();

        let users = guard
            .iter()
            .map(|(k, _)| Ok(String::from(k)))
            .collect::<Vec<_>>();

        DynIter::from(users)
    }
}

pub fn user_service() -> UserService {
    let store = MockUserStore::default();
    UserService::new(Box::from(store))
}
