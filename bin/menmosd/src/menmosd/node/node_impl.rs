use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, ensure, Result};
use async_trait::async_trait;
use bitvec::prelude::*;

use interface::{
    BlobInfo, BlobMetaRequest, DirectoryNode, FacetResponse, Hit, MetadataList, Query,
    QueryResponse, RoutingConfig, StorageNodeInfo, StorageNodeResponseData,
};

use rapidquery::Resolver;

use indexer::iface::*;

use super::routing::NodeRouter;

pub struct Directory<I>
where
    I: IndexProvider + Flush + Send + Sync,
{
    index: Arc<I>,
    node_router: NodeRouter<I::RoutingProvider>,
    rebuild_queue: Mutex<Vec<StorageNodeInfo>>,
}

impl<I> Directory<I>
where
    I: IndexProvider + Flush + Send + Sync,
{
    pub fn new(index: I) -> Self {
        let index_arc = Arc::from(index);
        let node_router = NodeRouter::new(index_arc.routing());
        Self {
            index: index_arc,
            node_router,
            rebuild_queue: Default::default(),
        }
    }

    fn load_document(&self, idx: u32) -> Result<Hit> {
        let doc = self.index.documents().lookup(idx)?;
        ensure!(doc.is_some(), "missing document");

        let info = self.index.meta().get(idx)?;
        ensure!(info.is_some(), "missing blob info");

        Ok(Hit::new(
            doc.unwrap(),
            info.unwrap().meta,
            String::default(),
        )) // TODO: This default string isn't super clean, but in the current architecture its guaranteed to be replaced before returning.
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
        let rebuild_requested = {
            let rebuild_queue_guard = self.rebuild_queue.lock().map_err(|e| anyhow!("{}", e))?;

            if let Some(storage_node) = rebuild_queue_guard.last() {
                storage_node.id == def.id
            } else {
                false
            }
        };

        self.node_router.add_node(def).await;

        Ok(StorageNodeResponseData { rebuild_requested })
    }

    async fn pick_node_for_blob(
        &self,
        blob_id: &str,
        meta: BlobMetaRequest,
        username: &str,
    ) -> Result<StorageNodeInfo> {
        self.node_router.route_blob(blob_id, &meta, username).await
    }

    async fn get_blob_meta(&self, blob_id: &str, username: &str) -> Result<Option<BlobInfo>> {
        let blob_idx_maybe = self.index.documents().get(blob_id)?;
        let blob_info_maybe = blob_idx_maybe
            .map(|i| self.index.meta().get(i))
            .transpose()?
            .flatten();

        if let Some(info) = &blob_info_maybe {
            if info.owner != username {
                return Ok(None);
            }
        }

        Ok(blob_info_maybe)
    }

    async fn get_blob_storage_node(&self, blob_id: &str) -> Result<Option<StorageNodeInfo>> {
        // These next two lines could technically be in one line, but since its an async
        // function and index::storage() returns a ref to the storage provider, the borrow checker can't guarantee that there
        // won't be concurrent accesses to the storage provider. Doing it in two lines makes it explicit that the ref. to the
        // storage provider is dropped before the await point.
        let node_id_maybe = self.index.storage().get_node_for_blob(&blob_id)?;
        if let Some(node_id) = node_id_maybe {
            Ok(self.node_router.get_node(&node_id).await)
        } else {
            Ok(None)
        }
    }

    async fn index_blob(&self, blob_id: &str, info: BlobInfo, storage_node_id: &str) -> Result<()> {
        self.index
            .storage()
            .set_node_for_blob(blob_id, storage_node_id.to_string())?;

        // TODO: Figure out a way to implement transactions here, so that a failed insert won't pollute the document index..
        let doc_idx = self.index.documents().insert(blob_id)?;
        self.index.meta().insert(doc_idx, &info)?;

        Ok(())
    }

    async fn start_rebuild(&self) -> Result<()> {
        let storage_nodes = self.node_router.list_nodes().await;
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

    async fn get_routing_config(&self, user: &str) -> Result<Option<RoutingConfig>> {
        self.index.routing().get_routing_config(user)
    }

    async fn set_routing_config(&self, user: &str, routing_config: &RoutingConfig) -> Result<()> {
        self.index
            .routing()
            .set_routing_config(user, routing_config)
    }

    async fn delete_routing_config(&self, user: &str) -> Result<()> {
        self.index.routing().delete_routing_config(user)
    }

    async fn delete_blob(
        &self,
        blob_id: &str,
        storage_node_id: &str,
    ) -> Result<Option<StorageNodeInfo>> {
        let node_maybe = self.index.storage().get_node_for_blob(blob_id)?;

        if let Some(node) = node_maybe {
            ensure!(
                node == storage_node_id,
                "blob node is different from the storage node id that requested deletion"
            )
        } else {
            log::error!("no node found for blob_id={}", blob_id)
        }

        // This is tricky, because since our internal document IDs are sequential, we can't just delete the blob from the index and call it a day.
        // Also, since we have a limit of u32::MAX document IDs, we'd better recycle those deleted IDs so we don't creep
        // towards the limit too fast in delete-heavy implementations.
        //
        // This is the algorithm:
        //  - Delete the BlobID from the storage node index.
        let node_id_maybe = self.index.storage().delete_blob(blob_id)?;

        //  - Delete the BlobID -> BlobIDX mapping in the document index.
        //  - Add the BlobIDX to a list of "free" indices (up for recycling) kept in the document index.
        if let Some(blob_idx) = self.index.documents().delete(blob_id)? {
            //  - If the blob exists in the index we need to "purge" the BlobIDX:
            //  - For all tag, k/v and parent values in the metadata index, set to `0` the bit corresponding to the deleted BlobIDX.
            //  - Once a new insert comes, prioritize using a recycled BlobIDX over allocating a new one.
            self.index.meta().purge(blob_idx)?;
        }

        if let Some(node_id) = node_id_maybe {
            Ok(self.node_router.get_node(&node_id).await)
        } else {
            Ok(None)
        }
    }

    async fn query(&self, query: &Query, username: &str) -> Result<QueryResponse> {
        let result_bitvector =
            query.expression.evaluate(self)? & self.index.meta().load_user_mask(username)?;

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
        username: &str,
    ) -> Result<MetadataList> {
        let user_mask = self.index.meta().load_user_mask(username)?;

        let tag_list = match tags.as_ref() {
            Some(tag_filters) => {
                let mut hsh = HashMap::with_capacity(tag_filters.len());
                for tag in tag_filters {
                    hsh.insert(
                        tag.clone(),
                        (self.index.meta().load_tag(tag)? & user_mask.clone()).count_ones(),
                    );
                }
                hsh
            }
            None => self.index.meta().list_all_tags(Some(&user_mask))?,
        };

        let kv_list = self
            .index
            .meta()
            .list_all_kv_fields(&meta_keys, Some(&user_mask))?;

        Ok(MetadataList {
            tags: tag_list,
            meta: kv_list,
        })
    }

    async fn list_storage_nodes(&self) -> Result<Vec<StorageNodeInfo>> {
        Ok(self.node_router.list_nodes().await)
    }

    async fn login(&self, user: &str, password: &str) -> Result<bool> {
        self.index.users().authenticate(user, password)
    }

    async fn register(&self, user: &str, password: &str) -> Result<()> {
        self.index.users().add_user(user, password)
    }

    async fn has_user(&self, user: &str) -> Result<bool> {
        self.index.users().has_user(user)
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
