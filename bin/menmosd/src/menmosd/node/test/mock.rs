use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Mutex,
};

use anyhow::{ensure, Result};

use async_trait::async_trait;

use bitvec::prelude::*;

use chrono::Utc;

use interface::{BlobInfo, StorageNodeInfo};

use indexer::iface::*;

fn tag_to_kv(tag: &str) -> Result<(&str, &str)> {
    let splitted: Vec<_> = tag.split('$').collect();
    ensure!(splitted.len() == 2, "invalid kv tag");
    Ok((splitted[0], splitted[1]))
}

#[derive(Default)]
pub struct MockDocIDMap {
    forward_map: Mutex<HashMap<String, u32>>,
    backward_map: Mutex<HashMap<u32, String>>,
    recycled_ids: Mutex<Vec<u32>>,
    next_id: AtomicU32,
}

impl DocIDMapper for MockDocIDMap {
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
pub struct MockStorageMap {
    m: Mutex<HashMap<String, String>>,
    nodes: Mutex<HashMap<String, (StorageNodeInfo, chrono::DateTime<Utc>)>>,
}

impl StorageNodeMapper for MockStorageMap {
    fn get_node(&self, node_id: &str) -> Result<Option<(StorageNodeInfo, chrono::DateTime<Utc>)>> {
        let guard = self.nodes.lock().unwrap();
        let map = &*guard;
        Ok(map.get(node_id).cloned())
    }

    fn get_all_nodes(&self) -> Result<Vec<StorageNodeInfo>> {
        let guard = self.nodes.lock().unwrap();
        let map = &*guard;
        Ok(map
            .iter()
            .map(|(_node_id, (node_info, _last_seen))| node_info.clone())
            .collect())
    }

    fn write_node(&self, info: StorageNodeInfo, seen_at: chrono::DateTime<Utc>) -> Result<bool> {
        let mut guard = self.nodes.lock().unwrap();
        let map = &mut *guard;
        let was_set = map.insert(info.id.clone(), (info, seen_at)).is_some();
        Ok(was_set)
    }

    fn delete_node(&self, node_id: &str) -> Result<()> {
        let mut guard = self.nodes.lock().unwrap();
        guard.remove(node_id);
        Ok(())
    }

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
pub struct MockMetaMap {
    meta_map: Mutex<HashMap<u32, BlobInfo>>,
    tag_map: Mutex<HashMap<String, BitVec>>,
}

impl MetadataMapper for MockMetaMap {
    fn get(&self, idx: u32) -> Result<Option<BlobInfo>> {
        let guard = self.meta_map.lock().unwrap();
        let map = &*guard;
        Ok(map.get(&idx).cloned())
    }

    fn insert(&self, id: u32, info: &BlobInfo) -> Result<()> {
        let mut meta_guard = self.meta_map.lock().unwrap();
        let mut tag_guard = self.tag_map.lock().unwrap();
        let meta_map = &mut *meta_guard;
        let tag_map = &mut *tag_guard;

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

    fn list_all_tags(&self) -> Result<HashMap<String, usize>> {
        let tag_guard = self.tag_map.lock().unwrap();
        let tag_map = &*tag_guard;

        let mut hsh = HashMap::with_capacity(tag_map.len());

        for (k, v) in tag_map.iter() {
            if !k.contains('$') && !k.contains('!') {
                hsh.insert(k.clone(), v.count_ones());
            }
        }

        Ok(hsh)
    }

    fn list_all_kv_fields(
        &self,
        key_filter: &Option<Vec<String>>,
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
                        hsh.entry(key.clone())
                            .or_insert_with(HashMap::default)
                            .insert(val.to_string(), bitvec.count_ones());
                    }
                }
            }
            None => {
                for ((key, val), bv) in tag_map
                    .iter()
                    .filter(|(k, _v)| k.contains('$'))
                    .map(|(k, v)| (tag_to_kv(k).unwrap(), v))
                {
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
pub struct MockUserMap {
    users: Mutex<HashMap<String, String>>,
}

impl UserMapper for MockUserMap {
    fn authenticate(&self, username: &str, password: &str) -> Result<bool> {
        let guard = self.users.lock().unwrap();
        Ok(guard.get(username).cloned().unwrap_or(String::default()) == password)
    }

    fn add_user(&self, username: &str, password: &str) -> Result<()> {
        let mut guard = self.users.lock().unwrap();
        guard.insert(username.to_string(), password.to_string());
        Ok(())
    }
}

#[derive(Default)]
pub struct MockIndex {
    documents: MockDocIDMap,
    meta: MockMetaMap,
    storage: MockStorageMap,
    users: MockUserMap,
}

#[async_trait]
impl Flush for MockIndex {
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

impl IndexProvider for MockIndex {
    type MetadataProvider = MockMetaMap;
    type DocumentProvider = MockDocIDMap;
    type StorageProvider = MockStorageMap;
    type UserProvider = MockUserMap;

    fn documents(&self) -> &Self::DocumentProvider {
        &self.documents
    }

    fn meta(&self) -> &Self::MetadataProvider {
        &self.meta
    }

    fn storage(&self) -> &Self::StorageProvider {
        &self.storage
    }

    fn users(&self) -> &Self::UserProvider {
        &self.users
    }
}
