use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;

use parking_lot::Mutex;

pub struct TransferGuard {
    blob_id: String,
    pending_transfers: Arc<Mutex<HashSet<String>>>,
}

impl Drop for TransferGuard {
    fn drop(&mut self) {
        let mut guard = self.pending_transfers.lock();
        guard.remove(&self.blob_id);
    }
}

#[derive(Default)]
pub struct PendingTransfers {
    data: Arc<Mutex<HashSet<String>>>,
}

impl PendingTransfers {
    pub fn start(&self, blob_id: &str) -> Result<Option<TransferGuard>> {
        let mut guard = self.data.lock();

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
