use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};

pub struct TransferGuard {
    blob_id: String,
    pending_transfers: Arc<Mutex<HashSet<String>>>,
}

impl Drop for TransferGuard {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.pending_transfers.lock() {
            guard.remove(&self.blob_id);
        } else {
            tracing::error!("error during drop: poisoned mutex");
        }
    }
}

#[derive(Default)]
pub struct PendingTransfers {
    data: Arc<Mutex<HashSet<String>>>,
}

impl PendingTransfers {
    pub fn start(&self, blob_id: &str) -> Result<Option<TransferGuard>> {
        let mut guard = self.data.lock().map_err(|e| anyhow!("{}", e.to_string()))?;

        if guard.contains(blob_id) {
            return Ok(None);
        }

        (*guard).insert(String::from(blob_id));

        Ok(Some(TransferGuard {
            blob_id: String::from(blob_id),
            pending_transfers: self.data.clone(),
        }))
    }
}
