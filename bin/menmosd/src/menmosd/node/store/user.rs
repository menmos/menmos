use anyhow::{anyhow, Result};

use async_trait::async_trait;

use ring::rand::{SecureRandom, SystemRandom};

use super::iface::Flush;
use super::DynIter;

const REGISTERED_USERS_MAP: &str = "registered_users";

pub trait UserStore: Flush {
    fn add_user(&self, username: &str, password: &str) -> Result<()>;
    fn authenticate(&self, username: &str, password: &str) -> Result<bool>;
    fn has_user(&self, username: &str) -> Result<bool>;
    fn iter(&self) -> DynIter<'static, Result<String>>;
}

pub struct SledUserStore {
    map: sled::Tree,
    rng: SystemRandom,
}

impl SledUserStore {
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
impl Flush for SledUserStore {
    async fn flush(&self) -> Result<()> {
        self.map.flush_async().await?;
        Ok(())
    }
}

impl UserStore for SledUserStore {
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

    fn has_user(&self, username: &str) -> Result<bool> {
        let user_exists = self.map.contains_key(username.as_bytes())?;

        Ok(user_exists)
    }

    fn iter(&self) -> DynIter<'static, Result<String>> {
        DynIter::new(self.map.iter().map(|pair_result| {
            pair_result
                .map(|(key_ivec, _val_ivec)| String::from_utf8_lossy(&key_ivec).to_string())
                .map_err(|e| e.into())
        }))
    }
}
