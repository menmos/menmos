use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use lfan::preconfig::concurrent::{new_ttl_cache, TTLLRUCache};
use menmos_client::{Client, Meta, Query, QueryResponse};

use tokio::sync::Mutex;

static META_TTL: Duration = Duration::from_secs(30 * 60); // 30 min.
static QUERY_TTL: Duration = Duration::from_secs(30);

pub struct CachedClient {
    client: Client,

    meta_cache: TTLLRUCache<String, Option<Meta>>,
    query_cache: TTLLRUCache<Query, QueryResponse>,
}

impl CachedClient {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            meta_cache: new_ttl_cache(10000, META_TTL),
            query_cache: new_ttl_cache(50, QUERY_TTL),
        }
    }

    pub async fn get_meta(&self, blob_id: &str) -> Result<Option<Meta>> {
        match self.meta_cache.get(blob_id).await {
            Some(meta_maybe) => Ok(meta_maybe),
            None => {
                let meta_maybe = self.client.get_meta(blob_id).await?;
                self.meta_cache
                    .insert(String::from(blob_id), meta_maybe.clone())
                    .await;
                Ok(meta_maybe)
            }
        }
    }

    pub async fn query(&self, query: Query) -> Result<QueryResponse> {
        let query_response = {
            match self.query_cache.get(&query).await {
                Some(query_response) => query_response,
                None => {
                    let response = self.client.query(query.clone()).await?;
                    self.query_cache.insert(query, response.clone()).await;
                    response
                }
            }
        };

        // Since query results come with the blob meta, we can insert each blob's meta in the cache directly, making subsequent individual file lookups
        // much faster.
        self.meta_cache
            .batch_insert(
                query_response
                    .hits
                    .iter()
                    .map(|hit| (hit.id.clone(), Some(hit.meta.clone()))),
            )
            .await;

        Ok(query_response)
    }

    pub async fn read_range(&self, blob_id: &str, range: (u64, u64)) -> Result<Vec<u8>> {
        // Not cached for now.
        Ok(self.client.read_range(blob_id, range).await?)
    }

    pub async fn create_empty(&self, meta: Meta) -> Result<String> {
        self.query_cache.clear().await;
        Ok(self.client.create_empty(meta).await?)
    }

    pub async fn write(&self, blob_id: &str, offset: u64, buffer: Bytes) -> Result<()> {
        self.meta_cache.clear().await;
        Ok(self.client.write(blob_id, offset, buffer).await?)
    }

    pub async fn update_meta(&self, blob_id: &str, meta: Meta) -> Result<()> {
        self.query_cache.clear().await;
        self.client.update_meta(blob_id, meta).await?;
        Ok(())
    }

    pub async fn fsync(&self, blob_id: &str) -> Result<()> {
        self.client.fsync(blob_id).await?;
        Ok(())
    }

    pub async fn delete(&self, blob_id: String) -> Result<()> {
        // a delete is obviously not cached, but it _does_ invalidate our query cache.
        self.query_cache.clear().await;
        Ok(self.client.delete(blob_id).await?)
    }
}
