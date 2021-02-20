use anyhow::{anyhow, Result};

use async_trait::async_trait;

use ring::rand::{SecureRandom, SystemRandom};

use crate::iface::{Flush, UserMapper};

const REGISTERED_USERS_MAP: &str = "registered_users";

pub struct UsersStore {
    map: sled::Tree,
    rng: SystemRandom,
}

impl UsersStore {
    pub fn new(db: &sled::Db) -> Result<Self> {
        let map = db.open_tree(REGISTERED_USERS_MAP)?;

        let rng = SystemRandom::new();

        Ok(Self { map, rng })
    }

    fn generate_salt(&self) -> Result<[u8; 16]> {
        let mut salt = [0u8; 16];
        self.rng.fill(&mut salt).map_err(|e| anyhow!("{}", e))?;
        Ok(salt)
    }
}

#[async_trait]
impl Flush for UsersStore {
    async fn flush(&self) -> Result<()> {
        self.map.flush_async().await?;
        Ok(())
    }
}

impl UserMapper for UsersStore {
    fn add_user(&self, username: &str, password: &str) -> Result<()> {
        let config = argon2::Config::default();
        let password_hash =
            argon2::hash_encoded(password.as_bytes(), &self.generate_salt()?, &config).unwrap();

        self.map
            .insert(username.as_bytes(), password_hash.as_bytes())?;

        Ok(())
    }

    fn authenticate(&self, username: &str, password: &str) -> Result<bool> {
        if let Some(value) = self.map.get(username.as_bytes())? {
            let pw_hash = String::from_utf8_lossy(value.as_ref());
            Ok(argon2::verify_encoded(&pw_hash, password.as_bytes())?)
        } else {
            Ok(false)
        }
    }
}
