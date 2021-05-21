use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc, Mutex,
};

use anyhow::{ensure, Result};

use async_trait::async_trait;

use bitvec::prelude::*;

use interface::{BlobInfo, RoutingConfigState};

use indexer::iface::*;

fn tag_to_kv(tag: &str) -> Result<(&str, &str)> {
    let splitted: Vec<_> = tag.split('$').collect();
    ensure!(splitted.len() == 2, "invalid kv tag");
    Ok((splitted[0], splitted[1]))
}

#[derive(Default)]
pub struct MockDocIdMap {
    forward_map: Mutex<HashMap<String, u32>>,
    backward_map: Mutex<HashMap<u32, String>>,
    recycled_ids: Mutex<Vec<u32>>,
    next_id: AtomicU32,
}

impl DocIdMapper for MockDocIdMap {
    fn get_nb_of_docs(&self) -> u32 {
        self.next_id.load(Ordering::SeqCst)
    }

    fn insert(&self, doc_id: &str) -> Result<u32> {
        let mut fwd_guard = self.forward_map.lock().unwrap();
        let mut rwd_guard = self.backward_map.lock().unwrap();
        let mut recycled_guard = self.recycled_ids.lock().unwrap();

        let fwd_map = &mut *fwd_guard;
        let rwd_map = &mut *rwd_guard;
        let recycled = &mut *recycled_guard;

        if let Some(i) = fwd_map.get(doc_id) {
            Ok(*i)
        } else {
            let current_id = if let Some(id) = recycled.pop() {
                id
            } else {
                self.next_id.fetch_add(1, Ordering::SeqCst)
            };
            fwd_map.insert(String::from(doc_id), current_id);
            rwd_map.insert(current_id, String::from(doc_id));
            Ok(current_id)
        }
    }

    fn lookup(&self, doc_idx: u32) -> Result<Option<String>> {
        let rwd_guard = self.backward_map.lock().unwrap();
        let rwd_map = &*rwd_guard;

        if let Some(d) = rwd_map.get(&doc_idx) {
            Ok(Some(d.clone()))
        } else {
            Ok(None)
        }
    }

    fn delete(&self, doc_id: &str) -> Result<Option<u32>> {
        let mut fwd_guard = self.forward_map.lock().unwrap();
        let mut rwd_guard = self.backward_map.lock().unwrap();
        let mut recycled_guard = self.recycled_ids.lock().unwrap();

        let fwd_map = &mut *fwd_guard;
        let rwd_map = &mut *rwd_guard;
        let recycled = &mut *recycled_guard;

        if let Some(doc_idx) = fwd_map.remove(doc_id) {
            rwd_map.remove(&doc_idx);
            recycled.push(doc_idx);
            Ok(Some(doc_idx))
        } else {
            Ok(None)
        }
    }

    fn get_all_documents_mask(&self) -> Result<BitVec> {
        let recycled_guard = self.recycled_ids.lock().unwrap();
        let recycled = &*recycled_guard;

        // Initialize our bitvector with 1.
        let nb_of_docs = self.get_nb_of_docs() as usize;
        let mut initial_bv = bitvec![Lsb0, usize; 1; nb_of_docs];

        for idx in recycled.iter() {
            initial_bv.set(*idx as usize, false);
        }
        Ok(initial_bv)
    }

    fn get(&self, doc_id: &str) -> Result<Option<u32>> {
        let fwd_guard = self.forward_map.lock().unwrap();
        Ok((*fwd_guard).get(doc_id).cloned())
    }

    fn clear(&self) -> Result<()> {
        let mut fwd_guard = self.forward_map.lock().unwrap();
        let mut rwd_guard = self.backward_map.lock().unwrap();
        let mut recycled_guard = self.recycled_ids.lock().unwrap();
        fwd_guard.clear();
        rwd_guard.clear();
        recycled_guard.clear();
        Ok(())
    }
}

#[derive(Default)]
pub struct MockRoutingMap {
    m: Mutex<HashMap<String, RoutingConfigState>>,
}

impl RoutingMapper for MockRoutingMap {
    fn get_routing_config(&self, username: &str) -> Result<Option<RoutingConfigState>> {
        let guard = self.m.lock().unwrap();
        Ok(guard.get(username).cloned())
    }

    fn set_routing_config(
        &self,
        username: &str,
        routing_config: &RoutingConfigState,
    ) -> Result<()> {
        let mut guard = self.m.lock().unwrap();
        guard.insert(String::from(username), routing_config.clone());
        Ok(())
    }

    fn delete_routing_config(&self, username: &str) -> Result<()> {
        let mut guard = self.m.lock().unwrap();
        guard.remove(username);
        Ok(())
    }

    fn iter(&self) -> DynIter<'static, Result<RoutingConfigState>> {
        unimplemented!()
    }
}

#[derive(Default)]
pub struct MockStorageMap {
    m: Mutex<HashMap<String, String>>,
}

impl StorageNodeMapper for MockStorageMap {
    fn get_node_for_blob(&self, blob_id: &str) -> Result<Option<String>> {
        let guard = self.m.lock().unwrap();
        let map = &*guard;
        Ok(map.get(blob_id).cloned())
    }

    fn set_node_for_blob(&self, blob_id: &str, node_id: String) -> Result<()> {
        let mut guard = self.m.lock().unwrap();
        let map = &mut *guard;
        map.insert(String::from(blob_id), node_id);
        Ok(())
    }

    fn delete_blob(&self, blob_id: &str) -> Result<Option<String>> {
        let mut guard = self.m.lock().unwrap();
        let map = &mut *guard;
        Ok(map.remove(blob_id))
    }

    fn clear(&self) -> Result<()> {
        let mut guard = self.m.lock().unwrap();
        guard.clear();
        Ok(())
    }
}


#[derive(Default)]
pub struct MockIndex {
    documents: Arc<MockDocIdMap>,
    meta: Arc<MockMetaMap>,
    routing: Arc<MockRoutingMap>,
    storage: Arc<MockStorageMap>,
    users: Arc<MockUserMap>,
}

#[async_trait]
impl Flush for MockIndex {
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

impl IndexProvider for MockIndex {
    type MetadataProvider = MockMetaMap;
    type DocumentProvider = MockDocIdMap;
    type RoutingProvider = MockRoutingMap;
    type StorageProvider = MockStorageMap;
    type UserProvider = MockUserMap;

    fn documents(&self) -> Arc<Self::DocumentProvider> {
        self.documents.clone()
    }

    fn meta(&self) -> Arc<Self::MetadataProvider> {
        self.meta.clone()
    }

    fn routing(&self) -> Arc<Self::RoutingProvider> {
        self.routing.clone()
    }

    fn storage(&self) -> Arc<Self::StorageProvider> {
        self.storage.clone()
    }

    fn users(&self) -> Arc<Self::UserProvider> {
        self.users.clone()
    }
}
