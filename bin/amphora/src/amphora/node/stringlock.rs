use std::mem;
use std::sync::Arc;
use std::time::Instant;
use std::{collections::HashMap, time::Duration};

use tokio::sync::{Mutex, MutexGuard, RwLock};

static TOKIO_MUTEX_SIZE: usize = mem::size_of::<Mutex<()>>();

struct StringLockEntry {
    lock: Arc<RwLock<()>>,
    last_accessed: Instant,
}

impl Default for StringLockEntry {
    fn default() -> Self {
        Self {
            lock: Default::default(),
            last_accessed: Instant::now(),
        }
    }
}

pub struct StringLock {
    data: Mutex<HashMap<String, StringLockEntry>>,
    lifetime: Duration,
    cleanup_trigger: Option<usize>,
}

impl StringLock {
    pub fn new(lifetime: Duration) -> Self {
        Self {
            data: Default::default(),
            lifetime,
            cleanup_trigger: None,
        }
    }

    pub fn with_cleanup_trigger(mut self, max_memory: usize) -> Self {
        let trigger_size = max_memory / TOKIO_MUTEX_SIZE;
        self.cleanup_trigger = Some(trigger_size);
        self
    }

    fn trigger_cleanup_if_required(
        &self,
        guard: &mut MutexGuard<HashMap<String, StringLockEntry>>,
    ) {
        if let Some(trigger) = self.cleanup_trigger {
            if guard.len() > trigger {
                log::debug!(
                    "map size is over threshold - file lock cleanup triggered automatically"
                );
                self.cleanup_internal(guard);
            }
        }
    }

    pub async fn get_lock<S: AsRef<str>>(&self, key: S) -> Arc<RwLock<()>> {
        let mut data_guard = self.data.lock().await;

        self.trigger_cleanup_if_required(&mut data_guard);

        if let Some(entry) = data_guard.get_mut(key.as_ref()) {
            entry.last_accessed = Instant::now();
            entry.lock.clone()
        } else {
            let entry = StringLockEntry::default();
            let lock_copy = entry.lock.clone();
            data_guard.insert(String::from(key.as_ref()), entry);
            lock_copy
        }
    }

    fn cleanup_internal(&self, guard: &mut MutexGuard<HashMap<String, StringLockEntry>>) {
        let now = Instant::now();
        guard.retain(|_key, entry| now.duration_since(entry.last_accessed) < self.lifetime);
        let end_time = Instant::now();

        log::debug!(
            "file lock cleanup took {}ms",
            end_time.duration_since(now).as_millis()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn basic_get_works() {
        let lock_map = StringLock::new(Duration::from_secs(1));
        {
            let mtx = lock_map.get_lock("mykey").await;
            let _r_guard = mtx.read().await;
        }

        let guard = lock_map.data.lock().await;
        assert_eq!(guard.len(), 1);
    }

    #[tokio::test]
    async fn locking_same_key_does_not_add_again_to_map() {
        let lock_map = StringLock::new(Duration::from_secs(1));

        for _ in 0..5 {
            let mtx = lock_map.get_lock("always_same_key").await;
            let _r_guard = mtx.read().await;
        }

        {
            let guard = lock_map.data.lock().await;
            assert_eq!(guard.len(), 1);
        }
    }

    #[tokio::test]
    async fn cleanup_triggered_when_capacity_exceeded() {
        const TTL_MS: u64 = 10;

        let lock_map = StringLock::new(Duration::from_millis(TTL_MS))
            .with_cleanup_trigger(TOKIO_MUTEX_SIZE * 5);

        for i in 0..6 {
            let mtx = lock_map.get_lock(format!("key_{}", i)).await;
            let _r_guard = mtx.read().await;
        }

        {
            let guard = lock_map.data.lock().await;
            assert_eq!(guard.len(), 6);
        }

        tokio::time::sleep(Duration::from_millis(TTL_MS)).await;

        let _lock = lock_map.get_lock("key_that_will_trigger_cleanup").await;
        {
            let guard = lock_map.data.lock().await;
            assert_eq!(guard.len(), 1);
        }
    }
}
