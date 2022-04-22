use anyhow::Result;

use async_trait::async_trait;

use bitvec::prelude::*;

use super::id_map::IDMap;
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

pub struct SledDocumentIdStore {
    doc_id_map: IDMap,
}

impl SledDocumentIdStore {
    pub fn new(db: &sled::Db) -> Result<Self> {
        let doc_id_map = IDMap::new(db, "documents")?;
        Ok(Self { doc_id_map })
    }
}

#[async_trait]
impl Flush for SledDocumentIdStore {
    async fn flush(&self) -> Result<()> {
        tracing::debug!("beginning flush");
        self.doc_id_map.flush().await?;
        tracing::debug!("flush complete");
        Ok(())
    }
}

impl DocumentIdStore for SledDocumentIdStore {
    fn get_nb_of_docs(&self) -> u32 {
        self.doc_id_map.id_count()
    }

    #[tracing::instrument(name = "docid.insert", level = "debug", skip(self))]
    fn insert(&self, doc_id: &str) -> Result<u32> {
        self.doc_id_map.get_or_assign(doc_id.as_bytes())
    }

    #[tracing::instrument(name = "docid.get", level = "debug", skip(self))]
    fn get(&self, doc_id: &str) -> Result<Option<u32>> {
        self.doc_id_map.get(doc_id.as_bytes())
    }

    #[tracing::instrument(name = "docid.lookup", level = "debug", skip(self))]
    fn lookup(&self, doc_idx: u32) -> Result<Option<String>> {
        Ok(self.doc_id_map.lookup(doc_idx)?.map(|doc_id_bytes| {
            String::from_utf8(doc_id_bytes.to_vec()).expect("document ID is not UTF-8")
        }))
    }

    #[tracing::instrument(name = "docid.delete", level = "debug", skip(self))]
    fn delete(&self, doc_id: &str) -> Result<Option<u32>> {
        self.doc_id_map.delete(doc_id.as_bytes())
    }

    #[tracing::instrument(name = "docid.get_all_documents_mask", level = "debug", skip(self))]
    fn get_all_documents_mask(&self) -> Result<BitVec> {
        // Initialize our bitvector with 1.
        let nb_of_docs = self.get_nb_of_docs() as usize;
        let mut initial_bv = bitvec![usize, Lsb0; 1; nb_of_docs];

        tokio::task::block_in_place(|| {
            for idx in self.doc_id_map.recycling_iter() {
                let idx = idx? as usize;
                if idx < initial_bv.len() {
                    initial_bv.set(idx, false);
                }
            }
            Ok(initial_bv)
        })
    }

    #[tracing::instrument(name = "docid.clear", skip(self))]
    fn clear(&self) -> Result<()> {
        self.doc_id_map.clear()?;
        tracing::debug!("document index destroyed");
        Ok(())
    }
}
