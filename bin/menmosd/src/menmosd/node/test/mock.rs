use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{ensure, Result};

use async_trait::async_trait;

use bitvec::prelude::*;
use interface::{
    BlobIndexer, BlobInfo, NodeAdminController, QueryExecutor, RoutingConfigManager,
    RoutingConfigState, UserManagement,
};

use crate::node::{
    service::{IndexerService, NodeAdminService, QueryService},
    store::{
        iface::{DocumentIdStore, MetadataStore, StorageMappingStore},
        DynIter,
    },
};
use crate::{
    node::{
        routing::NodeRouter,
        service::{RoutingService, UserService},
        store::iface::{
            DynDocumentIDStore, DynMetadataStore, DynRoutingStore, DynStorageMappingStore, Flush,
            RoutingStore, UserStore,
        },
    },
    Directory,
};

fn tag_to_kv(tag: &str) -> Result<(&str, &str)> {
    let splitted: Vec<_> = tag.split('$').collect();
    ensure!(splitted.len() == 2, "invalid kv tag");
    Ok((splitted[0], splitted[1]))
}

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
        Ok(guard.get(username).cloned().unwrap_or_default() == password)
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

#[derive(Default)]
struct MockDocumentIDStore {
    forward_map: Mutex<HashMap<String, u32>>,
    backward_map: Mutex<HashMap<u32, String>>,
    recycled_ids: Mutex<Vec<u32>>,
    next_id: AtomicU32,
}

#[async_trait]
impl Flush for MockDocumentIDStore {
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

impl DocumentIdStore for MockDocumentIDStore {
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
pub struct MockMetadataStore {
    meta_map: Mutex<HashMap<u32, BlobInfo>>,
    tag_map: Mutex<HashMap<String, BitVec>>,
    users_map: Mutex<HashMap<String, BitVec>>,
}

#[async_trait]
impl Flush for MockMetadataStore {
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

impl MetadataStore for MockMetadataStore {
    fn get(&self, idx: u32) -> Result<Option<BlobInfo>> {
        let guard = self.meta_map.lock().unwrap();
        let map = &*guard;
        Ok(map.get(&idx).cloned())
    }

    fn insert(&self, id: u32, info: &BlobInfo) -> Result<()> {
        let mut meta_guard = self.meta_map.lock().unwrap();
        let mut tag_guard = self.tag_map.lock().unwrap();
        let mut users_guard = self.users_map.lock().unwrap();
        let meta_map = &mut *meta_guard;
        let tag_map = &mut *tag_guard;
        let users_map = &mut *users_guard;

        if let Some(bv) = users_map.get_mut(&info.owner) {
            if bv.len() <= id as usize {
                bv.resize(id as usize + 1, false);
            }
            bv.set(id as usize, true);
        } else {
            let mut bv = bitvec![Lsb0, usize; 0; id as usize + 1];
            bv.set(id as usize, true);
            users_map.insert(info.owner.clone(), bv);
        }

        meta_map.insert(id, info.clone());

        let mut taglist = info.meta.tags.clone();
        for (k, v) in info.meta.metadata.iter() {
            taglist.push(format!("{}${}", k, v));
        }
        for p in info.meta.parents.iter() {
            taglist.push(format!("__parent!{}", p));
        }

        for tag in taglist.into_iter() {
            if let Some(bv) = tag_map.get_mut(&tag) {
                if bv.len() <= id as usize {
                    bv.resize(id as usize + 1, false);
                }
                bv.set(id as usize, true);
            } else {
                let mut bv = bitvec![Lsb0, usize; 0; id as usize + 1];
                bv.set(id as usize, true);
                tag_map.insert(tag.clone(), bv);
            }
        }

        Ok(())
    }

    fn load_user_mask(&self, username: &str) -> Result<BitVec> {
        let users_guard = self.users_map.lock().unwrap();
        Ok(users_guard.get(username).cloned().unwrap_or_default())
    }

    fn load_tag(&self, tag: &str) -> Result<BitVec> {
        let tag_guard = self.tag_map.lock().unwrap();
        let tag_map = &*tag_guard;

        if let Some(s) = tag_map.get(tag) {
            Ok(s.clone())
        } else {
            Ok(BitVec::default())
        }
    }

    fn load_key_value(&self, k: &str, v: &str) -> Result<BitVec> {
        self.load_tag(&format!("{}${}", k, v))
    }

    fn load_key(&self, k: &str) -> Result<BitVec> {
        let tag_guard = self.tag_map.lock().unwrap();
        let tag_map = &*tag_guard;

        let mut bv = BitVec::default();
        for (_, v) in tag_map
            .clone()
            .into_iter()
            .filter(|(key, _)| key.starts_with(&format!("{}$", k)))
        {
            let (biggest, smallest) = if bv.len() > v.len() { (bv, v) } else { (v, bv) };
            bv = biggest;
            bv |= smallest;
        }

        Ok(bv)
    }

    fn load_children(&self, parent_id: &str) -> Result<BitVec> {
        self.load_tag(&format!("__parent!{}", parent_id))
    }

    fn list_all_tags(&self, user_bv: Option<&BitVec>) -> Result<HashMap<String, usize>> {
        let tag_guard = self.tag_map.lock().unwrap();
        let tag_map = &*tag_guard;

        let mut hsh = HashMap::with_capacity(tag_map.len());

        for (k, v) in tag_map.iter() {
            let bv = match user_bv {
                Some(u) => u.clone() & v.clone(),
                None => v.clone(),
            };

            if !k.contains('$') && !k.contains('!') {
                hsh.insert(k.clone(), bv.count_ones());
            }
        }

        Ok(hsh)
    }

    fn list_all_kv_fields(
        &self,
        key_filter: &Option<Vec<String>>,
        user_bv: Option<&BitVec>,
    ) -> Result<HashMap<String, HashMap<String, usize>>> {
        let tag_guard = self.tag_map.lock().unwrap();
        let tag_map = &*tag_guard;

        let mut hsh = HashMap::new();

        match key_filter {
            Some(f) => {
                for key in f.iter() {
                    for (val, bitvec) in tag_map
                        .iter()
                        .filter(|(k, _v)| k.starts_with(&format!("{}$", &key)))
                        .map(|(k, v)| (tag_to_kv(k).unwrap().1, v))
                    {
                        let bv = match user_bv {
                            Some(u) => u.clone() & bitvec.clone(),
                            None => bitvec.clone(),
                        };
                        hsh.entry(key.clone())
                            .or_insert_with(HashMap::default)
                            .insert(val.to_string(), bv.count_ones());
                    }
                }
            }
            None => {
                for ((key, val), bv) in tag_map
                    .iter()
                    .filter(|(k, _v)| k.contains('$'))
                    .map(|(k, v)| (tag_to_kv(k).unwrap(), v))
                {
                    let bv = match user_bv {
                        Some(u) => u.clone() & bv.clone(),
                        None => bv.clone(),
                    };
                    hsh.entry(key.to_string())
                        .or_insert_with(HashMap::default)
                        .insert(val.to_string(), bv.count_ones());
                }
            }
        }

        Ok(hsh)
    }

    fn purge(&self, idx: u32) -> Result<()> {
        let mut tag_guard = self.tag_map.lock().unwrap();
        let tag_map = &mut *tag_guard;
        for bitvec in tag_map.iter_mut().map(|v| v.1) {
            bitvec.set(idx as usize, false);
        }

        Ok(())
    }

    fn clear(&self) -> Result<()> {
        let mut meta_guard = self.meta_map.lock().unwrap();
        let mut tag_guard = self.tag_map.lock().unwrap();
        meta_guard.clear();
        tag_guard.clear();
        Ok(())
    }
}

#[derive(Default)]
pub struct MockStorageStore {
    m: Mutex<HashMap<String, String>>,
}

#[async_trait]
impl Flush for MockStorageStore {
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

impl StorageMappingStore for MockStorageStore {
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
pub struct MockRoutingStore {
    m: Mutex<HashMap<String, RoutingConfigState>>,
}

impl RoutingStore for MockRoutingStore {
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

#[async_trait]
impl Flush for MockRoutingStore {
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

pub fn node() -> Directory {
    let document_id_store: Arc<DynDocumentIDStore> =
        Arc::new(Box::from(MockDocumentIDStore::default()));

    let meta_store: Arc<DynMetadataStore> = Arc::new(Box::from(MockMetadataStore::default()));

    let storage_store: Arc<DynStorageMappingStore> =
        Arc::new(Box::from(MockStorageStore::default()));

    let routing_store: DynRoutingStore = Box::from(MockRoutingStore::default());

    let query_svc: Arc<Box<dyn QueryExecutor + Send + Sync>> =
        Arc::new(Box::from(QueryService::new(
            document_id_store.clone(),
            meta_store.clone(),
            storage_store.clone(),
        )));

    let users_svc: Arc<Box<dyn UserManagement + Send + Sync>> = Arc::new(Box::from(
        UserService::new(Box::from(MockUserStore::default())),
    ));

    let node_router = Arc::from(NodeRouter::new());

    let routing_svc: Arc<Box<dyn RoutingConfigManager + Send + Sync>> =
        Arc::new(Box::from(RoutingService::new(
            routing_store,
            document_id_store.clone(),
            meta_store.clone(),
            node_router.clone(),
            users_svc.clone(),
            query_svc.clone(),
        )));

    let index_svc: Arc<Box<dyn BlobIndexer + Send + Sync>> =
        Arc::new(Box::from(IndexerService::new(
            document_id_store,
            meta_store,
            storage_store,
            routing_svc.clone(),
            node_router.clone(),
        )));

    let admin_svc: Arc<Box<dyn NodeAdminController + Send + Sync>> = Arc::new(Box::from(
        NodeAdminService::new(index_svc.clone(), node_router),
    ));

    Directory::new(index_svc, routing_svc, admin_svc, users_svc, query_svc)
}
