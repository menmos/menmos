use std::{
    collections::{HashMap, HashSet},
    ops::Index,
};
use std::{io::SeekFrom, sync::Arc};
use std::{os::unix::prelude::MetadataExt, path::PathBuf};

use anyhow::{anyhow, Result};

use bytes::Bytes;
use futures::{
    future::{AbortHandle, Abortable},
    stream::FuturesUnordered,
    Stream, StreamExt,
};

use indexer::RecyclingIDGenerator;
use menmos_client::{Client, Meta};
use tokio::sync::{Mutex, MutexGuard};
use tokio::{
    fs,
    io::{AsyncSeekExt, AsyncWriteExt},
    task::JoinHandle,
};

pub struct CacheEntry {
    file: Mutex<fs::File>,

    path: PathBuf,

    blob_id: String,

    meta: Meta,

    open_descriptors: Mutex<HashSet<u64>>,
}

impl CacheEntry {
    pub async fn write_stream<S, E>(&self, stream: S) -> Result<()>
    where
        S: Stream<Item = Result<Bytes, E>>,
        E: ToString,
    {
        // TODO: Some kind of library to read/write streams to files in this fashion would be useful instead of doing it by hand everywhere...
        let mut stream_pin = Box::pin(stream);
        let mut file_guard = self.file.lock().await;

        while let Some(chunk) = stream_pin.next().await {
            match chunk {
                Ok(c) => file_guard.write_all(c.as_ref()).await?,
                Err(e) => {
                    return Err(anyhow!("{}", e.to_string()));
                }
            }
        }

        Ok(())
    }

    pub async fn write_at(&self, offset: u64, buffer: &[u8]) -> Result<()> {
        let mut file_guard = self.file.lock().await;
        file_guard.seek(SeekFrom::Start(offset)).await?;
        file_guard.write_all(buffer.as_ref()).await?;
        Ok(())
    }

    pub async fn add_fetch(&self, fh: u64) -> usize {
        let mut guard = self.open_descriptors.lock().await;
        guard.insert(fh);
        guard.len()
    }

    pub async fn remove_fetch(&self, fh: u64) -> usize {
        let mut guard = self.open_descriptors.lock().await;
        guard.remove(&fh);
        guard.len()
    }
}

struct UploadHandle {
    upload: Abortable<JoinHandle<()>>,
    abort_handle: AbortHandle,
}

#[derive(Default)]
struct CacheState {
    pub file_handles: HashMap<u64, String>,
    pub cache_entries: HashMap<String, CacheEntry>,
    pub uploads: HashMap<String, UploadHandle>,
}

pub struct WritebackCache {
    cache_dir: PathBuf,
    descriptor_recycler: RecyclingIDGenerator,
    client: Client,

    state: Arc<Mutex<CacheState>>,
}

impl WritebackCache {
    pub async fn new<P: Into<PathBuf>>(path: P, client: Client) -> Result<Self> {
        let cache_dir: PathBuf = path.into();
        fs::create_dir_all(&cache_dir).await?;

        Ok(Self {
            cache_dir,
            descriptor_recycler: RecyclingIDGenerator::new(1),
            client,
            state: Default::default(),
        })
    }

    pub async fn open(&self, blob_id: String, meta: Meta) -> Result<u64> {
        let file_descriptor = self.descriptor_recycler.get()? as u64;

        let mut state_lock = self.state.lock().await;

        if let Some(entry) = state_lock.cache_entries.get(&blob_id) {
            // Simple. Add a new file descriptor to the cache entry.
            entry.add_fetch(file_descriptor).await;
        } else {
            // Must create a new entry.
            let file_path = self
                .cache_dir
                .join(format!("{}_{}", file_descriptor, blob_id));

            let stream = self.client.get_file(&blob_id).await?;

            let file = fs::File::create(&file_path).await?;
            let mut open_descriptors = HashSet::new();
            open_descriptors.insert(file_descriptor);
            let entry = CacheEntry {
                file: Mutex::from(file),
                path: file_path,
                blob_id: blob_id.clone(),
                meta,
                open_descriptors: Mutex::from(open_descriptors),
            };

            entry.write_stream(stream).await?;

            state_lock.cache_entries.insert(blob_id.clone(), entry);
            state_lock.file_handles.insert(file_descriptor, blob_id);
        }

        Ok(file_descriptor)
    }

    fn get_entry<'a>(
        &self,
        file_descriptor: u64,
        guard: &'a MutexGuard<'a, CacheState>,
    ) -> Option<&'a CacheEntry> {
        let blob_id = guard.file_handles.get(&file_descriptor)?;
        guard.cache_entries.get(blob_id)
    }

    pub async fn write_at(
        &self,
        file_descriptor: u64,
        offset: u64,
        buffer: &[u8],
    ) -> Result<Option<()>> {
        let mut state_guard = self.state.lock().await;

        let blob_id = {
            let entry = match self.get_entry(file_descriptor, &state_guard) {
                Some(e) => e,
                None => {
                    return Ok(None);
                }
            };
            entry.write_at(offset, buffer).await?;
            entry.blob_id.clone()
        };

        // Abort pending upload, if any.
        if let Some(upload_handle) = state_guard.uploads.remove(&blob_id) {
            upload_handle.abort_handle.abort();
            upload_handle.upload.await;
        }

        Ok(Some(()))
    }

    pub async fn close(&self, file_descriptor: u64) -> Result<()> {
        let mut state_guard = self.state.lock().await;

        let entry = self
            .get_entry(file_descriptor, &state_guard)
            .ok_or_else(|| anyhow!("missing entry for file descriptor"))?;

        let upload_handle_pair = if entry.remove_fetch(file_descriptor).await == 0 {
            // Trigger an async upload of the file.
            {
                // Flush the file to disk.
                let file_lock = entry.file.lock().await;
                file_lock.sync_all().await?;
            }
            let client = self.client.clone();
            let item_size = entry.path.metadata()?.size();
            let meta = entry.meta.clone().with_size(item_size);
            let blob_id = entry.blob_id.clone();
            let path = entry.path.clone();

            let upload_handle = tokio::task::spawn(async move {
                // Update the blob.
                // TODO: Add retries.
                log::info!("beginning sync of {} from writeback cache", &blob_id);
                if let Err(e) = client.update_blob(&blob_id, &path, meta).await {
                    log::error!("failed to sync: {}", e);
                } else {
                    log::info!("flushed entry {} from writeback cache", &blob_id);
                }
            });

            let (abort_handle, abort_registration) = AbortHandle::new_pair();
            let upload = Abortable::new(upload_handle, abort_registration);
            Some((
                entry.blob_id.clone(),
                UploadHandle {
                    upload,
                    abort_handle,
                },
            ))
        } else {
            None
        };

        if let Some((blob_id, upload_handle)) = upload_handle_pair {
            state_guard.uploads.insert(blob_id, upload_handle);
        }

        Ok(())
    }
}
