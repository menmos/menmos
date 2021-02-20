use std::collections::{HashMap, LinkedList};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, ensure, Result};
use async_trait::async_trait;
use bitvec::prelude::*;
use chrono::Duration;

use interface::{
    BlobMeta, DirectoryNode, FacetResponse, Hit, MetadataList, Query, QueryResponse,
    StorageNodeInfo, StorageNodeResponseData,
};

use rapidquery::Resolver;

use indexer::iface::*;

const NODE_FORGET_DURATION_SECONDS: i64 = 60;

pub struct Directory<I>
where
    I: IndexProvider + Flush + Send + Sync,
{
    index: Arc<I>,
    nodes_round_robin: Mutex<LinkedList<String>>, // TODO: This could be implemented as a NodeSelectionStrategy that _could_ use the document meta to choose.
    node_forget_duration: chrono::Duration,
    rebuild_queue: Mutex<Vec<StorageNodeInfo>>,
}

impl<I> Directory<I>
where
    I: IndexProvider + Flush + Send + Sync,
{
    pub fn new(index: I) -> Self {
        Self {
            index: Arc::from(index),
            nodes_round_robin: Default::default(),
            node_forget_duration: Duration::seconds(NODE_FORGET_DURATION_SECONDS),
            rebuild_queue: Default::default(),
        }
    }

    fn get_node_if_fresh(&self, node_id: &str) -> Result<Option<StorageNodeInfo>> {
        if let Some((node_info, seen_at)) = self.index.storage().get_node(&node_id)? {
            if chrono::Utc::now() - seen_at > self.node_forget_duration {
                // Node is expired.
                Ok(None)
            } else {
                Ok(Some(node_info))
            }
        } else {
            Ok(None)
        }
    }

    fn prune_last_node(&self) -> Result<()> {
        let mut guard = self
            .nodes_round_robin
            .lock()
            .map_err(|_| anyhow!("poisoned mutex"))?;

        if let Some(node_id) = (*guard).pop_back() {
            self.index.storage().delete_node(&node_id)?;
        } else {
            log::warn!("called prune_last_node with an empty node list")
        }
        Ok(())
    }

    fn pick_node(&self, _meta: &BlobMeta) -> Result<StorageNodeInfo> {
        loop {
            // Get the node ID.
            let node_id = {
                let mut guard = self
                    .nodes_round_robin
                    .lock()
                    .map_err(|_| anyhow!("poisoned mutex"))?;
                let round_robin = &mut guard;
                let node_id = round_robin
                    .pop_front()
                    .ok_or_else(|| anyhow!("No storage node defined"))?;

                // Push back to update the round-robin
                round_robin.push_back(node_id.clone());

                node_id
            };

            // Fetch the storage node associated with the ID.
            if let Some(node) = self.get_node_if_fresh(&node_id)? {
                return Ok(node);
            } else {
                // Node is non-existent or stale.
                self.prune_last_node()?;
            }
        }
    }

    fn load_document(&self, idx: u32) -> Result<Hit> {
        let doc = self.index.documents().lookup(idx)?;
        ensure!(doc.is_some(), "missing document");

        let meta = self.index.meta().get(idx)?;
        ensure!(meta.is_some(), "missing meta");

        Ok(Hit::new(doc.unwrap(), meta.unwrap(), String::default())) // TODO: This default string isn't super clean, but in the current architecture its guaranteed to be replaced before returning.
    }
}

#[async_trait]
impl<I> DirectoryNode for Directory<I>
where
    I: IndexProvider + Flush + Send + Sync,
{
    async fn commit(&self) -> Result<()> {
        log::info!("beginning commit");
        self.index.flush().await?;
        log::info!("commit complete");
        Ok(())
    }

    async fn register_storage_node(
        &self,
        def: interface::StorageNodeInfo,
    ) -> Result<StorageNodeResponseData> {
        let id = def.id.clone();

        let already_existed = self.index.storage().write_node(def, chrono::Utc::now())?;

        let rebuild_requested = {
            let rebuild_queue_guard = self.rebuild_queue.lock().map_err(|e| anyhow!("{}", e))?;

            if let Some(storage_node) = rebuild_queue_guard.last() {
                storage_node.id == id
            } else {
                false
            }
        };

        // Register the node to the round robin if its new.
        if !already_existed {
            let mut guard = self
                .nodes_round_robin
                .lock()
                .map_err(|_| anyhow!("poisoned mutex"))?;

            let round_robin = &mut *guard;
            round_robin.push_back(id);
        }

        Ok(StorageNodeResponseData { rebuild_requested })
    }

    async fn add_blob(&self, _blob_id: &str, meta: BlobMeta) -> Result<StorageNodeInfo> {
        self.pick_node(&meta)
    }

    async fn get_blob_meta(&self, blob_id: &str) -> Result<Option<BlobMeta>> {
        self.index
            .documents()
            .get(blob_id)?
            .map(|blob_idx| self.index.meta().get(blob_idx))
            .transpose()
            .map(|result_maybe| result_maybe.flatten())
    }

    async fn get_blob_storage_node(&self, blob_id: &str) -> Result<Option<StorageNodeInfo>> {
        if let Some(node_id) = self.index.storage().get_node_for_blob(&blob_id)? {
            if let Some(node) = self.get_node_if_fresh(&node_id)? {
                Ok(Some(node))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    async fn index_blob(&self, blob_id: &str, meta: BlobMeta, storage_node_id: &str) -> Result<()> {
        self.index
            .storage()
            .set_node_for_blob(blob_id, storage_node_id.to_string())?;

        // TODO: Figure out a way to implement transactions here, so that a failed insert won't pollute the document index..
        let doc_idx = self.index.documents().insert(blob_id)?;
        self.index.meta().insert(doc_idx, &meta)?;

        Ok(())
    }

    async fn start_rebuild(&self) -> Result<()> {
        let storage_nodes = self.index.storage().get_all_nodes()?;
        log::info!("starting rebuild for {} nodes", storage_nodes.len());

        log::debug!("nuking the whole index");
        self.index.meta().clear()?;
        self.index.documents().clear()?;
        self.index.storage().clear()?;

        let mut rebuild_queue_guard = self.rebuild_queue.lock().map_err(|e| anyhow!("{}", e))?;
        (&mut *rebuild_queue_guard).extend(storage_nodes.into_iter());

        Ok(())
    }

    async fn rebuild_complete(&self, storage_node_id: &str) -> Result<()> {
        let mut rebuild_queue_guard = self.rebuild_queue.lock().map_err(|e| anyhow!("{}", e))?;
        rebuild_queue_guard.retain(|item| item.id != storage_node_id);
        log::info!("finished rebuild for node id={}", storage_node_id);
        Ok(())
    }

    async fn delete_blob(&self, blob_id: &str) -> Result<Option<StorageNodeInfo>> {
        // This is tricky, because since our internal document IDs are sequential, we can't just delete the blob from the index and call it a day.
        // Also, since we have a limit of u32::MAX document IDs, we'd better recycle those deleted IDs so we don't creep
        // towards the limit too fast in delete-heavy implementations.
        //
        // This is the algorithm:
        //  - Delete the BlobID from the storage node index.
        let storage_node_info = self
            .index
            .storage()
            .delete_blob(blob_id)?
            .map(|node_id| {
                self.index
                    .storage()
                    .get_node(&node_id)
                    .ok()
                    .flatten()
                    .map(|i| i.0)
            })
            .flatten();

        //  - Delete the BlobID -> BlobIDX mapping in the document index.
        //  - Add the BlobIDX to a list of "free" indices (up for recycling) kept in the document index.
        if let Some(blob_idx) = self.index.documents().delete(blob_id)? {
            //  - If the blob exists in the index we need to "purge" the BlobIDX:
            //  - For all tag, k/v and parent values in the metadata index, set to `0` the bit corresponding to the deleted BlobIDX.
            //  - Once a new insert comes, prioritize using a recycled BlobIDX over allocating a new one.
            self.index.meta().purge(blob_idx)?;
        }
        Ok(storage_node_info)
    }

    async fn query(&self, query: &Query) -> Result<QueryResponse> {
        let result_bitvector = query.expression.evaluate(self)?;

        // The number of true bits in the bitvector is the total number of query hits.
        let total = result_bitvector.count_ones(); // Total number of query hits.
        let count = query.size.min(total); // Number of returned query hits (paging).
        let mut hits = Vec::with_capacity(count);

        let mut facets = None;

        if total > 0 {
            // Get the numerical indices of all documents in the bitvector.
            let indices: Vec<u32> = result_bitvector.iter_ones().map(|e| e as u32).collect();

            // Compute facets on-the-fly
            // TODO: Facets could be made much faster via a structure at indexing time, this is a WIP.
            if query.facets {
                let mut tag_map = HashMap::new();
                let mut kv_map = HashMap::new();

                for idx in indices.iter() {
                    let doc = self.load_document(*idx)?;
                    for tag in doc.meta.tags.iter() {
                        let count = tag_map.entry(tag.clone()).or_insert(0);
                        *count += 1;
                    }

                    for (key, value) in doc.meta.metadata.iter() {
                        let entry_map = kv_map.entry(key.clone()).or_insert_with(HashMap::new);
                        let count = entry_map.entry(value.clone()).or_insert(0);
                        *count += 1;
                    }
                }

                facets = Some(FacetResponse {
                    tags: tag_map,
                    meta: kv_map,
                })
            }

            // Compute our bounds (from & size) according to the query.
            let start_point = query.from.min(total - 1);
            let end_point = (start_point + query.size).min(total);

            // Load _only_ the documents that will be returned by the query.
            for idx in &indices[start_point..end_point] {
                hits.push(self.load_document(*idx)?);
            }
        }

        Ok(QueryResponse {
            count,
            total,
            hits,
            facets,
        })
    }

    async fn list_metadata(
        &self,
        tags: Option<Vec<String>>,
        meta_keys: Option<Vec<String>>,
    ) -> Result<MetadataList> {
        let tag_list = match tags.as_ref() {
            Some(tag_filters) => {
                let mut hsh = HashMap::with_capacity(tag_filters.len());
                for tag in tag_filters {
                    hsh.insert(tag.clone(), self.index.meta().load_tag(tag)?.count_ones());
                }
                hsh
            }
            None => self.index.meta().list_all_tags()?,
        };

        let kv_list = self.index.meta().list_all_kv_fields(&meta_keys)?;

        Ok(MetadataList {
            tags: tag_list,
            meta: kv_list,
        })
    }

    async fn list_storage_nodes(&self) -> Result<Vec<StorageNodeInfo>> {
        let guard = self
            .nodes_round_robin
            .lock()
            .map_err(|_| anyhow!("poisoned mutex"))?;

        let node_list = &*guard;

        let mut node_infos = Vec::new();
        for node_id in node_list.iter() {
            if let Some((info, _last_seen)) = self.index.storage().get_node(node_id)? {
                // TODO: Returning last seen here would be nice too.
                node_infos.push(info);
            }
        }

        Ok(node_infos)
    }

    async fn login(&self, user: &str, password: &str) -> Result<bool> {
        self.index.users().authenticate(user, password)
    }

    async fn register(&self, user: &str, password: &str) -> Result<()> {
        self.index.users().add_user(user, password)
    }
}

impl<I> Resolver<BitVec> for Directory<I>
where
    I: IndexProvider + Flush + Send + Sync,
{
    type Error = anyhow::Error;

    fn resolve_tag(&self, tag: &str) -> Result<BitVec, Self::Error> {
        self.index.meta().load_tag(tag)
    }

    fn resolve_key_value(&self, key: &str, value: &str) -> Result<BitVec, Self::Error> {
        self.index.meta().load_key_value(key, value)
    }

    fn resolve_key(&self, key: &str) -> Result<BitVec, Self::Error> {
        self.index.meta().load_key(key)
    }

    fn resolve_children(&self, parent_id: &str) -> Result<BitVec, Self::Error> {
        self.index.meta().load_children(parent_id)
    }

    fn resolve_empty(&self) -> Result<BitVec, Self::Error> {
        self.index.documents().get_all_documents_mask()
    }
}
