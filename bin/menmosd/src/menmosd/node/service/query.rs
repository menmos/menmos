use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, ensure, Result};

use async_trait::async_trait;

use bitvec::prelude::*;

use interface::{FacetResponse, Hit, MetadataList, Query, QueryResponse, SortOrder};

use crate::node::store::iface::{DynDocumentIDStore, DynMetadataStore, DynStorageMappingStore};

pub struct QueryService {
    documents: Arc<DynDocumentIDStore>,
    metadata: Arc<DynMetadataStore>,
    storage: Arc<DynStorageMappingStore>,
}

impl QueryService {
    pub fn new(
        documents: Arc<DynDocumentIDStore>,
        metadata: Arc<DynMetadataStore>,
        storage: Arc<DynStorageMappingStore>,
    ) -> Self {
        Self {
            documents,
            metadata,
            storage,
        }
    }

    #[tracing::instrument(name = "query.load_document", skip(self))]
    fn load_document(&self, idx: u32) -> Result<Hit> {
        let doc = self.documents.lookup(idx)?;
        ensure!(doc.is_some(), "missing document");

        let info = self.metadata.get(idx)?;
        ensure!(info.is_some(), "missing blob info");

        Ok(Hit::new(
            doc.unwrap(),
            info.unwrap().meta,
            String::default(),
        )) // TODO: This default string isn't super clean, but in the current architecture its guaranteed to be replaced before returning.
    }

    #[tracing::instrument(
        name = "query.get_bitvector",
        level = "info",
        skip(self, query, username)
    )]
    fn get_resulting_bitvector(&self, query: &Query, username: &str) -> Result<BitVec> {
        Ok(query.expression.evaluate(self)? & self.metadata.load_user_mask(username)?)
    }
}

#[async_trait]
impl interface::QueryExecutor for QueryService {
    #[tracing::instrument(name = "query.query", skip(self, query, username))]
    async fn query(&self, query: &Query, username: &str) -> Result<QueryResponse> {
        let result_bitvector = self.get_resulting_bitvector(query, username)?;
        tracing::debug!(count=?result_bitvector.count_ones(), "loaded resulting bitvector");

        // The number of true bits in the bitvector is the total number of query hits.
        let total = result_bitvector.count_ones(); // Total number of query hits.
        let count = query.size.min(total); // Number of returned query hits (paging).
        let mut hits = Vec::with_capacity(count);

        let mut facets = None;

        if total > 0 {
            // Get the numerical indices of all documents in the bitvector.
            let indices: Vec<u32> = {
                let mut ind = result_bitvector
                    .iter_ones()
                    .map(|e| e as u32)
                    .collect::<Vec<_>>();

                if query.sort_order == SortOrder::CreationDescending {
                    ind.reverse();
                }

                ind
            };

            // Compute facets on-the-fly
            // TODO: Facets could be made much faster via a structure at indexing time, this is a WIP.
            if query.facets {
                let span = tracing::info_span!("query.facets");
                span.in_scope(|| {
                    let mut tag_map = HashMap::new();
                    let mut kv_map = HashMap::new();

                    for idx in indices.iter() {
                        let doc = self.load_document(*idx)?;
                        for tag in doc.meta.tags.iter() {
                            let count = tag_map.entry(tag.clone()).or_insert(0);
                            *count += 1;
                        }

                        for (key, value) in doc.meta.fields.iter() {
                            let entry_map = kv_map.entry(key.clone()).or_insert_with(HashMap::new);
                            let count = entry_map.entry(value.clone()).or_insert(0);
                            *count += 1;
                        }
                    }

                    facets = Some(FacetResponse {
                        tags: tag_map,
                        meta: kv_map,
                    });
                    Ok::<_, anyhow::Error>(())
                })?;
            }

            // Compute our bounds (from & size) according to the query.
            let start_point = query.from.min(total - 1);
            let end_point = (start_point + query.size).min(total);

            // Load _only_ the documents that will be returned by the query.
            tracing::info_span!("query.load_documents").in_scope(|| {
                for idx in &indices[start_point..end_point] {
                    hits.push(self.load_document(*idx)?);
                }
                Ok::<_, anyhow::Error>(())
            })?;
        }

        Ok(QueryResponse {
            count,
            total,
            hits,
            facets,
        })
    }

    #[tracing::instrument(name = "query.query_move_requests", skip(self, query, username))]
    async fn query_move_requests(
        &self,
        query: &Query,
        username: &str,
        src_node: &str,
    ) -> Result<Vec<String>> {
        let resulting_bitvector = self.get_resulting_bitvector(query, username)?;

        let mut move_requests = Vec::new();

        if resulting_bitvector.count_ones() == 0 {
            // No pending move requests.
            return Ok(move_requests);
        }

        for doc_idx in resulting_bitvector.iter_ones() {
            let blob_id = self
                .documents
                .lookup(doc_idx as u32)?
                .ok_or_else(|| anyhow!("missing document ID for index '{}'", doc_idx))?;

            let blob_storage_node = self
                .storage
                .get_node_for_blob(&blob_id)?
                .ok_or_else(|| anyhow!("missing storage node for blob '{}'", blob_id))?;

            if blob_storage_node == src_node {
                move_requests.push(blob_id)
            }
        }

        Ok(move_requests)
    }

    #[tracing::instrument(name = "query.list_metadata", skip(self, tags, meta_keys, username))]
    async fn list_metadata(
        &self,
        tags: Option<Vec<String>>,
        meta_keys: Option<Vec<String>>,
        username: &str,
    ) -> Result<MetadataList> {
        let user_mask = self.metadata.load_user_mask(username)?;

        let tag_list = match tags.as_ref() {
            Some(tag_filters) => {
                let mut hsh = HashMap::with_capacity(tag_filters.len());
                for tag in tag_filters {
                    hsh.insert(
                        tag.clone(),
                        (self.metadata.load_tag(tag)? & user_mask.clone()).count_ones(),
                    );
                }
                hsh
            }
            None => self.metadata.list_all_tags(Some(&user_mask))?,
        };

        let kv_list = self
            .metadata
            .list_all_kv_fields(&meta_keys, Some(&user_mask))?;

        Ok(MetadataList {
            tags: tag_list,
            fields: kv_list,
        })
    }
}

impl rapidquery::FieldResolver<BitVec> for QueryService {
    type FieldType = interface::ExpressionField;
    type Error = anyhow::Error;

    fn resolve(&self, field: &Self::FieldType) -> std::result::Result<BitVec, Self::Error> {
        match field {
            Self::FieldType::Tag { tag } => self.metadata.load_tag(tag),
            Self::FieldType::Field { key, value } => self.metadata.load_key_value(key, value),
            Self::FieldType::HasField { key } => self.metadata.load_key(key),
        }
    }

    fn resolve_empty(&self) -> Result<BitVec, Self::Error> {
        self.documents.get_all_documents_mask()
    }
}
