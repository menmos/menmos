use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::Result;

use async_trait::async_trait;

use bitvec::prelude::*;

use byteorder::{BigEndian, ReadBytesExt};

use super::iface::Flush;

pub trait DocumentIdStore: Flush {
    fn get_nb_of_docs(&self) -> u32;
    fn insert(&self, doc_id: &str) -> Result<u32>;
    fn get(&self, doc_id: &str) -> Result<Option<u32>>;
    fn lookup(&self, doc_idx: u32) -> Result<Option<String>>;
    fn delete(&self, doc_id: &str) -> Result<Option<u32>>;
    fn get_all_documents_mask(&self) -> Result<BitVec>;
    fn clear(&self) -> Result<()>;
}

const DOC_MAP: &str = "document";
const DOC_REV_MAP: &str = "document-rev";
const RECYCLING_STORE: &str = "id-recycle";

pub struct SledDocumentIdStore {
    doc_map: sled::Tree,         // DocumentID => IDX
    doc_reverse_map: sled::Tree, // IDX => DocumentID
    recycling_store: sled::Tree,

    next_id: AtomicU32,
}

impl SledDocumentIdStore {
    pub fn new(db: &sled::Db) -> Result<Self> {
        let doc_map = db.open_tree(DOC_MAP)?;
        let doc_reverse_map = db.open_tree(DOC_REV_MAP)?;
        let recycling_store = db.open_tree(RECYCLING_STORE)?;

        let next_id = match doc_reverse_map.last()? {
            Some((k, _)) => k.as_ref().read_u32::<BigEndian>()? + 1,
            None => 0,
        };

        Ok(Self {
            doc_map,
            doc_reverse_map,
            recycling_store,
            next_id: AtomicU32::new(next_id),
        })
    }
}

#[async_trait]
impl Flush for SledDocumentIdStore {
    async fn flush(&self) -> Result<()> {
        log::debug!("beginning flush");
        self.doc_map.flush_async().await?;
        self.doc_reverse_map.flush_async().await?;
        log::debug!("flush complete");
        Ok(())
    }
}

impl DocumentIdStore for SledDocumentIdStore {
    fn get_nb_of_docs(&self) -> u32 {
        self.next_id.load(Ordering::SeqCst)
    }

    fn insert(&self, doc_id: &str) -> Result<u32> {
        if let Some(i) = self.doc_map.get(doc_id).unwrap() {
            Ok(i.as_ref().read_u32::<BigEndian>()?)
        } else {
            // Recycle an ID (if possible), else assign a new one.
            let current_id = if let Some((idx_ivec, _)) = self.recycling_store.pop_min()? {
                idx_ivec.as_ref().read_u32::<BigEndian>()?
            } else {
                self.next_id.fetch_add(1, Ordering::SeqCst)
            };

            let current_id_bytes = current_id.to_be_bytes();
            self.doc_map.insert(doc_id, &current_id_bytes.clone())?;
            self.doc_reverse_map
                .insert(current_id_bytes, doc_id.as_bytes())?;

            Ok(current_id)
        }
    }

    fn get(&self, doc_id: &str) -> Result<Option<u32>> {
        Ok(self
            .doc_map
            .get(doc_id.as_bytes())?
            .map(|idx_ivec| idx_ivec.as_ref().read_u32::<BigEndian>())
            .transpose()?)
    }

    fn lookup(&self, doc_idx: u32) -> Result<Option<String>> {
        if let Some(i) = self.doc_reverse_map.get(doc_idx.to_be_bytes())? {
            Ok(Some(String::from_utf8_lossy(i.as_ref()).to_string()))
        } else {
            Ok(None)
        }
    }

    fn delete(&self, doc_id: &str) -> Result<Option<u32>> {
        if let Some(doc_idx_ivec) = self.doc_map.remove(doc_id.as_bytes())? {
            let doc_idx = doc_idx_ivec.as_ref().read_u32::<BigEndian>()?;
            self.doc_reverse_map.remove(&doc_idx_ivec)?;
            self.recycling_store.insert(doc_idx_ivec, &[])?;
            Ok(Some(doc_idx))
        } else {
            Ok(None)
        }
    }

    fn get_all_documents_mask(&self) -> Result<BitVec> {
        // Initialize our bitvector with 1.
        let nb_of_docs = self.get_nb_of_docs() as usize;
        let mut initial_bv = bitvec![Lsb0, usize; 1; nb_of_docs];

        // This isn't _super_ efficient at query-time, but it makes indexing much quicker.
        // TODO: Improve the datastructure for keeping recycled IDs if this becomes a bottleneck.
        for idx_ivec in self
            .recycling_store
            .iter()
            .filter_map(|f| f.ok())
            .map(|p| p.0)
        {
            let idx = idx_ivec.as_ref().read_u32::<BigEndian>()? as usize;
            if idx < initial_bv.len() {
                initial_bv.set(idx, false);
            }
        }
        Ok(initial_bv)
    }

    fn clear(&self) -> Result<()> {
        self.next_id.store(0, Ordering::SeqCst);
        self.doc_map.clear()?;
        self.doc_reverse_map.clear()?;
        self.recycling_store.clear()?;
        log::debug!("document index destroyed");
        Ok(())
    }
}
