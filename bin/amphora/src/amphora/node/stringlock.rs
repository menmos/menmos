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
