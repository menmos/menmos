use std::sync::atomic::{AtomicU64, Ordering};
use std::{collections::HashMap, time::Instant};

use anyhow::{anyhow, ensure, Result};

use async_fuse::FileType;
use config::Contents;
use menmos_client::{Client, Meta, Query, Type};
use tokio::sync::Mutex;

use crate::{cached_client::CachedClient, concurrent_map::ConcurrentMap};
use crate::{config, write_buffer::WriteBuffer};

use super::virtualdir::VirtualDirectory;
use super::{Error, Result as FSResult};

pub struct MenmosFS {
    pub(crate) client: CachedClient,

    pub(crate) blobid_to_inode: ConcurrentMap<String, u64>,
    pub(crate) inode_to_blobid: ConcurrentMap<u64, String>,
    pub(crate) name_to_blobid: ConcurrentMap<(u64, String), String>,

    pub(crate) inode_to_last_refresh: ConcurrentMap<u64, Instant>,

    pub(crate) virtual_directories_inodes: ConcurrentMap<u64, VirtualDirectory>,
    pub(crate) virtual_directories: ConcurrentMap<(u64, String), u64>,

    pub(crate) write_buffers: Mutex<HashMap<u64, WriteBuffer>>,

    inode_counter: AtomicU64,
}

impl MenmosFS {
    pub async fn new(mount: config::Mount) -> Result<Self> {
        let client = match mount.client {
            config::ClientConfig::Profile { profile } => Client::new_with_profile(profile)?,
            config::ClientConfig::Host { host, password } => Client::new(host, password)?,
        };
        let client = CachedClient::new(client);

        let fs = Self {
            client,
            blobid_to_inode: Default::default(),
            inode_to_blobid: Default::default(),
            name_to_blobid: Default::default(),
            inode_counter: AtomicU64::new(3),

            inode_to_last_refresh: ConcurrentMap::new(),

            virtual_directories_inodes: ConcurrentMap::new(),
            virtual_directories: Default::default(),

            write_buffers: Default::default(),
        };

        // Initialize the filesystem roots.
        fs.virtual_directories_inodes
            .insert(
                1,
                VirtualDirectory::Mount {
                    contents: mount.contents,
                },
            )
            .await;
        Ok(fs)
    }

    /// Flushes the write buffer for an inode to the menmos cluster.
    ///
    /// Returns an IO error if the write fails.
    pub(crate) async fn flush_buffer(&self, ino: u64, buffer: WriteBuffer) -> FSResult<()> {
        let blob_id = self
            .inode_to_blobid
            .get(&ino)
            .await
            .ok_or(Error::NotFound)?;

        self.client
            .write(&blob_id, buffer.offset, buffer.data.freeze())
            .await
            .map_err(|e| {
                log::error!("write error: {}", e);
                Error::IOError
            })?;

        Ok(())
    }

    /// Gets the inode corresponding to a blob ID.
    ///
    /// If the blob ID wasn't seen before, a new inode is generated and returned.
    pub(crate) async fn get_inode(&self, blob_id: &str) -> u64 {
        if let Some(inode) = self.blobid_to_inode.get(&String::from(blob_id)).await {
            inode
        } else {
            let inode = self.inode_counter.fetch_add(1, Ordering::SeqCst);
            self.blobid_to_inode
                .insert(blob_id.to_string(), inode)
                .await;
            inode
        }
    }

    /// Gets a virtual directory by its parent inode and its name.
    ///
    /// If this directory wasn't seen before, a new inode is generated and returned.
    pub(crate) async fn get_virtual_inode(&self, parent_inode: u64, name: &str) -> u64 {
        if let Some(inode) = self
            .virtual_directories
            .get(&(parent_inode, name.to_string()))
            .await
        {
            inode
        } else {
            let inode = self.inode_counter.fetch_add(1, Ordering::SeqCst);
            self.virtual_directories
                .insert((parent_inode, name.to_string()), inode)
                .await;
            inode
        }
    }

    /// Get the metadata for a given inode.
    ///
    /// Returns None if the blob id corresponding on the server doesn't exist,
    /// returns an error if there is no blob ID corresponding to the provided inode.
    pub(crate) async fn get_meta_by_inode(&self, inode: u64) -> Result<Option<Meta>> {
        if let Some(blob_id) = self.inode_to_blobid.get(&inode).await {
            Ok(self.client.get_meta(&blob_id).await?)
        } else {
            Err(anyhow!("unknown inode"))
        }
    }

    /// Lists entries for a physical (backed by a blob on the menmos cluster) directory.
    pub(crate) async fn list_entries(
        &self,
        query: Query,
        parent_inode: u64,
    ) -> Result<Vec<(u64, FileType, String)>> {
        // TODO: Actually use paging here.
        let results = self.client.query(query.with_size(5000)).await?;

        // All directories have "." and ".."
        let mut entries = vec![
            (parent_inode, FileType::Directory, ".".to_string()),
            (parent_inode, FileType::Directory, "..".to_string()),
        ];
        entries.reserve(results.count);

        for hit in results.hits.into_iter() {
            let inode = self.get_inode(&hit.id).await;
            let file_type = if hit.meta.blob_type == menmos_client::Type::Directory {
                FileType::Directory
            } else {
                FileType::RegularFile
            };
            let blob_id = hit.id;
            let name = hit.meta.name.clone();
            self.name_to_blobid
                .insert((parent_inode, name.clone()), blob_id)
                .await;
            entries.push((inode, file_type, name))
        }

        Ok(entries)
    }

    /// Lists entries for a virtual (created on the client by a query) directory.
    pub(crate) async fn list_virtual_entries(
        &self,
        virtual_directory: VirtualDirectory,
        parent_inode: u64,
    ) -> Result<Vec<(u64, FileType, String)>> {
        match virtual_directory {
            VirtualDirectory::InMemory(v) => {
                // These are all other virtual directories.
                let mut entries = vec![
                    (parent_inode, FileType::Directory, ".".to_string()),
                    (parent_inode, FileType::Directory, "..".to_string()),
                ];

                entries.reserve(v.len());
                for dir_name in v.into_iter() {
                    let inode = self.get_virtual_inode(parent_inode, &dir_name).await;
                    entries.push((inode, FileType::Directory, dir_name));
                }

                Ok(entries)
            }
            VirtualDirectory::Query { query } => self.list_entries(query, parent_inode).await,
            VirtualDirectory::Mount { contents } => match contents {
                Contents::Root { root } => {
                    let mut entries = vec![
                        (parent_inode, FileType::Directory, ".".to_string()),
                        (parent_inode, FileType::Directory, "..".to_string()),
                    ];
                    let item_inode = self.get_inode(&root).await;
                    let meta = self
                        .client
                        .get_meta(&root)
                        .await?
                        .ok_or_else(|| anyhow!("mount meta does not exist"))?;
                    self.name_to_blobid
                        .insert((parent_inode, meta.name.clone()), root)
                        .await;
                    let t = if meta.blob_type == Type::File {
                        FileType::RegularFile
                    } else {
                        FileType::Directory
                    };
                    entries.push((item_inode, t, meta.name));

                    Ok(entries)
                }
                Contents::Virtual(mounts) => {
                    let mut entries = vec![
                        (parent_inode, FileType::Directory, ".".to_string()),
                        (parent_inode, FileType::Directory, "..".to_string()),
                    ];

                    // Create a virtual directory for each of our mounts.
                    for (k, contents) in mounts.into_iter() {
                        let mount_inode = self.get_virtual_inode(parent_inode, &k).await;

                        self.virtual_directories_inodes
                            .insert(mount_inode, VirtualDirectory::Mount { contents })
                            .await;
                        entries.push((mount_inode, FileType::Directory, k));
                    }

                    Ok(entries)
                }
                Contents::Query {
                    expression,
                    group_by_meta_keys,
                    group_by_tags,
                } => {
                    let root_query = Query::default().with_expression(expression)?;

                    let should_group = group_by_tags || !group_by_meta_keys.is_empty();

                    if should_group {
                        let mut entries = vec![
                            (parent_inode, FileType::Directory, ".".to_string()),
                            (parent_inode, FileType::Directory, "..".to_string()),
                        ];
                        self.populate_virtual_directories(root_query, parent_inode)
                            .await?;

                        // TODO: Will need to use a more unique inode name since we can nest.
                        let tags_inode = self.get_virtual_inode(parent_inode, "tags").await;
                        entries.push((tags_inode, FileType::Directory, "tags".to_string()));

                        for group_key in group_by_meta_keys.iter() {
                            let kv_inode = self.get_virtual_inode(parent_inode, group_key).await;
                            entries.push((kv_inode, FileType::Directory, group_key.clone()));
                        }

                        Ok(entries)
                    } else {
                        // Display the results as a flat list.
                        self.list_entries(root_query, parent_inode).await
                    }
                }
            },
        }
    }

    /// Populates the metadata maps for virtual directories.
    ///
    /// This creates the virtual subdirectories for `query` mounts, creating directories for each tag and k/v pair.
    pub(crate) async fn populate_virtual_directories(
        &self,
        mut query: Query,
        parent_inode: u64,
    ) -> Result<()> {
        query.facets = true;
        let results = self.client.query(query.clone()).await?;
        ensure!(results.facets.is_some(), "missing facets");

        let facets = results.facets.unwrap();

        // Build "tags" virtual directory.
        let tags: Vec<String> = facets.tags.into_iter().map(|(tag, _count)| tag).collect();

        let kv: HashMap<String, Vec<String>> = facets
            .meta
            .into_iter()
            .map(|(key, meta_counts)| {
                (
                    key,
                    meta_counts.into_iter().map(|(key, _count)| key).collect(),
                )
            })
            .collect();

        // Register the inode for our tag directory.
        let tags_inode = self.get_virtual_inode(parent_inode, "tags").await;
        self.virtual_directories_inodes
            .insert(tags_inode, VirtualDirectory::InMemory(tags.clone()))
            .await;

        // Register a virtual directory for every tag.
        for tag in tags.into_iter() {
            let tag_inode = self.get_virtual_inode(tags_inode, &tag).await;
            self.virtual_directories_inodes
                .insert(
                    tag_inode,
                    VirtualDirectory::Query {
                        query: query.clone().and_tag(tag.clone()),
                    },
                )
                .await;
        }

        // Build k/v virtual directories
        for (key, values) in kv.into_iter() {
            let key_inode = self.get_virtual_inode(parent_inode, &key).await;
            self.virtual_directories_inodes
                .insert(key_inode, VirtualDirectory::InMemory(values.clone()))
                .await;

            for value in values.into_iter() {
                let value_inode = self.get_virtual_inode(key_inode, &value).await;
                self.virtual_directories_inodes
                    .insert(
                        value_inode,
                        VirtualDirectory::Query {
                            query: query.clone().and_meta(key.clone(), value.clone()),
                        },
                    )
                    .await;
            }
        }

        Ok(())
    }
}
