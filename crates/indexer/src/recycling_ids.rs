use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

use anyhow::{anyhow, Result};

// TODO: Implement a persistent version of this for the document ID store.
pub struct RecyclingIDGenerator {
    next_id: AtomicU32,

    recycled_id_count: AtomicU32, // Used for checking if there are recycled IDs without locking
    recycled_ids: Mutex<Vec<u32>>,
}

impl RecyclingIDGenerator {
    pub fn new(next_id: u32) -> Self {
        Self {
            next_id: AtomicU32::new(next_id),
            recycled_id_count: AtomicU32::new(0),
            recycled_ids: Default::default(),
        }
    }

    fn pop_recycled_id(&self) -> Result<Option<u32>> {
        if self.recycled_id_count.load(Ordering::SeqCst) > 0 {
            let mut recycled_guard = self
                .recycled_ids
                .lock()
                .map_err(|e| anyhow!(e.to_string()))?;
            let id_maybe = recycled_guard.pop();
            if id_maybe.is_some() {
                self.recycled_id_count.fetch_sub(1, Ordering::SeqCst);
            }
            Ok(id_maybe)
        } else {
            Ok(None)
        }
    }

    pub fn get(&self) -> Result<u32> {
        match self.pop_recycled_id()? {
            Some(id) => Ok(id),
            None => Ok(self.next_id.fetch_add(1, Ordering::SeqCst)),
        }
    }

    pub fn put_back(&self, id: u32) -> Result<()> {
        let mut recycled_guard = self
            .recycled_ids
            .lock()
            .map_err(|e| anyhow!(e.to_string()))?;
        recycled_guard.push(id);
        Ok(())
    }
}
