use anyhow::Result;
use bytes::Buf;
use client::{Client, Meta, Query, QueryResponse};
use lfan::preconfig::LRUCache;
use tokio::sync::Mutex;

pub struct CachedClient {
    client: Client,

    meta_cache: Mutex<LRUCache<String, Option<Meta>>>,
    query_cache: Mutex<LRUCache<Query, QueryResponse>>,
}

impl CachedClient {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            meta_cache: Mutex::from(LRUCache::new(10000)),
            query_cache: Mutex::from(LRUCache::new(50)),
        }
    }

    pub async fn get_meta(&self, blob_id: &str) -> Result<Option<Meta>> {
        let mut cache_guard = self.meta_cache.lock().await;
        let cache = &mut *cache_guard;

        match cache.get(blob_id).cloned() {
            Some(meta_maybe) => Ok(meta_maybe),
            None => {
                let meta_maybe = self.client.get_meta(blob_id).await?;
                cache.insert(String::from(blob_id), meta_maybe.clone());
                Ok(meta_maybe)
            }
        }
    }

    pub async fn query(&self, query: Query) -> Result<QueryResponse> {
        let query_response = {
            let mut cache_guard = self.query_cache.lock().await;
            let cache = &mut *cache_guard;

            match cache.get(&query).cloned() {
                Some(query_response) => query_response,
                None => {
                    let response = self.client.query(query.clone()).await?;
                    cache.insert(query, response.clone());
                    response
                }
            }
        };

        // Since query results come with the blob meta, we can insert each blob's meta in the cache directly, making subsequent individual file lookups
        // much faster.
        let mut cache_guard = self.meta_cache.lock().await;
        let cache = &mut *cache_guard;
        for hit in query_response.hits.iter() {
            cache.insert(hit.id.clone(), Some(hit.meta.clone()));
        }

        Ok(query_response)
    }

    pub async fn read_range(&self, blob_id: &str, range: (u64, u64)) -> Result<Vec<u8>> {
        // Not cached for now.
        Ok(self.client.read_range(blob_id, range).await?)
    }

    pub async fn create_empty(&self, meta: Meta) -> Result<String> {
        {
            let mut cache_guard = self.query_cache.lock().await;
            let cache = &mut *cache_guard;
            cache.clear();
        }
        Ok(self.client.create_empty(meta).await?)
    }

    pub async fn write<B: Buf>(&self, blob_id: &str, offset: u64, buffer: B) -> Result<()> {
        {
            let mut cache_guard = self.meta_cache.lock().await;
            let cache = &mut *cache_guard;
            cache.invalidate(blob_id);
        }
        Ok(self.client.write(blob_id, offset, buffer).await?)
    }

    pub async fn update_meta(&self, blob_id: &str, meta: Meta) -> Result<()> {
        {
            let mut cache_guard = self.query_cache.lock().await;
            let cache = &mut *cache_guard;
            cache.clear();
        }
        self.client.update_meta(blob_id, meta).await?;
        Ok(())
    }

    pub async fn delete(&self, blob_id: String) -> Result<()> {
        // a delete is obviously not cached, but it _does_ invalidate our query cache.
        {
            let mut cache_guard = self.query_cache.lock().await;
            let cache = &mut *cache_guard;
            cache.clear();
        }
        Ok(self.client.delete(blob_id).await?)
    }
}
