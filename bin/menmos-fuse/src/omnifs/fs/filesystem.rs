use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{anyhow, ensure, Result};

use async_fuse::FileType;
use config::Contents;
use menmos_client::{Client, Meta, Query, Type};

use crate::config;
use crate::{cached_client::CachedClient, concurrent_map::ConcurrentMap};

use super::virtualdir::VirtualDirectory;

pub struct OmniFS {
    pub(crate) client: CachedClient,

    pub(crate) blobid_to_inode: ConcurrentMap<String, u64>,
    pub(crate) inode_to_blobid: ConcurrentMap<u64, String>,
    pub(crate) name_to_blobid: ConcurrentMap<(u64, String), String>,

    pub(crate) virtual_directories_inodes: ConcurrentMap<u64, VirtualDirectory>,
    pub(crate) virtual_directories: ConcurrentMap<(u64, String), u64>,

    inode_counter: AtomicU64,
}

impl OmniFS {
    pub async fn new(mount: config::Mount) -> Result<Self> {
        let client = CachedClient::new(Client::new_with_profile(mount.profile)?);

        let fs = Self {
            client,
            blobid_to_inode: Default::default(),
            inode_to_blobid: Default::default(),
            name_to_blobid: Default::default(),
            inode_counter: AtomicU64::new(3),

            virtual_directories_inodes: ConcurrentMap::new(),
            virtual_directories: Default::default(),
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

    pub(crate) async fn get_meta_by_inode(&self, inode: u64) -> Result<Option<Meta>> {
        if let Some(blob_id) = self.inode_to_blobid.get(&inode).await {
            Ok(self.client.get_meta(&blob_id).await?)
        } else {
            Err(anyhow!("unknown inode"))
        }
    }

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
                    log::info!("listing mount vdir");
                    let mut entries = vec![
                        (parent_inode, FileType::Directory, ".".to_string()),
                        (parent_inode, FileType::Directory, "..".to_string()),
                    ];

                    // Create a virtual directory for each of our mounts.
                    for (k, contents) in mounts.into_iter() {
                        let mount_inode = self.get_virtual_inode(parent_inode, &k).await;
                        log::info!("got parent inode: {}/{}: {}", parent_inode, &k, mount_inode);

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

    pub(crate) async fn read(&self, inode: u64, offset: i64, size: u32) -> Result<Option<Vec<u8>>> {
        ensure!(offset >= 0, "invalid offset");

        let blob_id = match self.inode_to_blobid.get(&inode).await {
            Some(blob_id) => blob_id,
            None => {
                return Ok(None);
            }
        };

        let bounds = (offset as u64, (offset + (size - 1) as i64) as u64);
        let bytes = self.client.read_range(&blob_id, bounds).await?;
        Ok(Some(bytes))
    }

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

    pub async fn rm_rf(&self, blob_id: &str) -> Result<()> {
        let mut working_stack = vec![(String::from(blob_id), Type::Directory)];

        while !working_stack.is_empty() {
            // Get a new root.
            let (target_id, blob_type) = working_stack.pop().unwrap();

            if blob_type == Type::Directory {
                // List the root's children.
                let results = self
                    .client
                    .query(Query::default().and_parent(&target_id).with_size(5000))
                    .await?;
                for hit in results.hits.into_iter() {
                    working_stack.push((hit.id, hit.meta.blob_type));
                }
            }

            // Delete the root.
            // TODO: Batch delete would be a nice addition.
            self.client.delete(target_id).await?;
        }

        Ok(())
    }

    pub async fn rename_blob(
        &self,
        source_parent_id: &str,
        source_blob: &str,
        new_name: &str,
        new_parent_id: &str,
    ) -> Result<()> {
        let mut source_meta = self
            .client
            .get_meta(&source_blob)
            .await?
            .ok_or_else(|| anyhow!("missing blob"))?;

        source_meta.name = new_name.into();
        source_meta
            .parents
            .retain(|item| item != source_parent_id && item != new_parent_id);
        source_meta.parents.push(new_parent_id.into());

        self.client.update_meta(source_blob, source_meta).await?;

        Ok(())
    }
}
