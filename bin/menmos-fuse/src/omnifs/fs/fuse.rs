use std::time::UNIX_EPOCH;
use std::{ffi::OsStr, time::SystemTime};

use async_fuse::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyWrite, Request,
};

use async_trait::async_trait;

use libc::{EACCES, ENOENT, O_CREAT, O_TRUNC};
use menmos_client::{Meta, Query, Type};

use crate::constants;
use crate::WriteBuffer;

use super::OmniFS;

fn build_attributes(inode: u64, meta: &Meta, perm: u16) -> FileAttr {
    let kind = match meta.blob_type {
        Type::Directory => FileType::Directory,
        Type::File => FileType::RegularFile,
    };

    FileAttr {
        ino: inode,
        size: meta.size,
        blocks: meta.size / constants::BLOCK_SIZE,
        atime: UNIX_EPOCH, // 1970-01-01 00:00:00
        mtime: UNIX_EPOCH,
        ctime: UNIX_EPOCH,
        crtime: UNIX_EPOCH,
        kind,
        perm,
        nlink: if kind == FileType::RegularFile {
            1
        } else {
            2 + meta.parents.len() as u32
        },
        uid: 1000,
        gid: 1000,
        rdev: 0,
        flags: 0,
    }
}

#[async_trait]
impl Filesystem for OmniFS {
    async fn lookup(&self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let str_name = name.to_string_lossy().to_string();

        // First, check if it's a virtual directory.
        {
            if let Some(inode) = self
                .virtual_directories
                .get(&(parent, str_name.clone()))
                .await
            {
                log::info!("lookup on {:?} found vdir inode: {}", name, inode,);
                let attrs = build_attributes(inode, &Meta::new(&str_name, Type::Directory), 0o444);
                reply.entry(&constants::TTL, &attrs, inode); // TODO: Replace the generation number by a nanosecond timestamp.
                return;
            }
        }

        // If not, proceed as usual.

        let blob_id = match self.name_to_blobid.get(&(parent, str_name.clone())).await {
            Some(b) => b,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        match self.client.get_meta(&blob_id).await {
            Ok(Some(blob_meta)) => {
                // We got the meta, time to make the item attribute.
                let inode = self.get_inode(&blob_id).await;
                let attributes = build_attributes(inode, &blob_meta, 0o764);
                reply.entry(&constants::TTL, &attributes, inode);
                log::info!(
                    "lookup on {:?} found inode: {} for ID {} ({:?})",
                    name,
                    inode,
                    blob_id,
                    blob_meta.blob_type
                );
                self.inode_to_blobid.insert(inode, blob_id).await;
            }
            Ok(None) => reply.error(ENOENT),
            Err(e) => {
                log::error!("lookup error: {}", e);
                reply.error(ENOENT)
            }
        }
    }

    async fn mkdir(
        &self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        reply: ReplyEntry,
    ) {
        let parent_blobid = match self.inode_to_blobid.get(&parent).await {
            Some(b) => b,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let str_name = name.to_string_lossy().to_string();
        let meta = Meta::new(str_name.clone(), Type::Directory).with_parent(parent_blobid);
        let blob_id = match self.client.create_empty(meta.clone()).await {
            Ok(b) => b,
            Err(e) => {
                log::error!("client error: {}", e);
                reply.error(EACCES);
                return;
            }
        };

        let ino = self.get_inode(&blob_id).await;
        self.inode_to_blobid.insert(ino, blob_id.clone()).await;
        self.name_to_blobid
            .insert((parent, str_name), blob_id)
            .await;

        reply.entry(&constants::TTL, &build_attributes(ino, &meta, 0o764), 0);
    }

    async fn rename(
        &self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEmpty,
    ) {
        log::info!(
            "rename {}/{:?} => {}/{:?}",
            parent,
            name,
            newparent,
            newname
        );

        // Does the source file exist?
        let src_name = name.to_string_lossy().to_string();
        let dst_name = newname.to_string_lossy().to_string();
        if let Some(source_blob) = self.name_to_blobid.get(&(parent, src_name.clone())).await {
            // Does the destination file exist?
            if let Some(dst_blob) = self
                .name_to_blobid
                .get(&(newparent, dst_name.clone()))
                .await
            {
                if let Some(inode) = self.blobid_to_inode.remove(&dst_blob).await {
                    self.inode_to_blobid.remove(&inode).await;
                }

                // If so, delete it before our rename.
                if let Err(e) = self.client.delete(dst_blob).await {
                    log::error!("client error: {}", e);
                    reply.error(ENOENT);
                    return;
                }
            }

            let source_parent_id = match self.inode_to_blobid.get(&parent).await {
                Some(p) => p,
                None => {
                    log::error!("parent inode doesn't exist");
                    reply.error(ENOENT);
                    return;
                }
            };

            // Does the parent inode exist?;
            if let Some(new_parent_id) = self.inode_to_blobid.get(&newparent).await {
                // Rename the blob.
                if let Err(e) = self
                    .rename_blob(&source_parent_id, &source_blob, &dst_name, &new_parent_id)
                    .await
                {
                    log::error!("client error: {}", e);
                    reply.error(ENOENT);
                } else {
                    self.name_to_blobid.remove(&(parent, src_name)).await;
                    self.name_to_blobid
                        .insert((newparent, dst_name), source_blob)
                        .await;

                    reply.ok();
                }
            } else {
                reply.error(ENOENT);
            }
        } else {
            reply.error(ENOENT);
            return;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn setattr(
        &self,
        _req: &Request,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<SystemTime>,
        _mtime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        // Size was already set on the server so we don't need
        // to actually update.
        self.getattr(_req, ino, reply).await
    }

    async fn getattr(&self, _req: &Request, ino: u64, reply: ReplyAttr) {
        if ino == 1 {
            reply.attr(&constants::TTL, &constants::ROOT_DIR_ATTR);
            return;
        }

        log::info!("getattr: {}", ino);

        // If virtual directory.
        if self.virtual_directories_inodes.get(&ino).await.is_some() {
            // TODO: Make a separate method to get attributes for virtual directories.
            let attrs = build_attributes(ino, &Meta::new("", Type::Directory), 0o444);
            reply.attr(&constants::TTL, &attrs);
            return;
        }

        match self.get_meta_by_inode(ino).await {
            Ok(Some(meta)) => {
                reply.attr(&constants::TTL, &build_attributes(ino, &meta, 0o764));
            }
            Ok(None) => {
                reply.error(ENOENT);
            }
            Err(e) => {
                log::error!("client error: {}", e);
                reply.error(ENOENT)
            }
        }
    }

    async fn read(
        &self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        reply: ReplyData,
    ) {
        match self.read(ino, offset, size).await {
            Ok(Some(bytes)) => {
                log::info!(
                    "read {}-{} on ino={} => got {} bytes",
                    offset,
                    (offset + size as i64),
                    ino,
                    bytes.len()
                );
                reply.data(&bytes);
            }
            Ok(None) => {
                reply.error(ENOENT);
            }
            Err(e) => {
                log::error!("read error: {}", e);
                reply.error(ENOENT);
            }
        }
    }

    async fn create(
        &self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _flags: u32,
        reply: ReplyCreate,
    ) {
        let str_name = name.to_string_lossy().to_string();
        log::info!("CREATE {}/{:?}", parent, &str_name);
        if let Some(blob_id) = self.name_to_blobid.get(&(parent, str_name)).await {
            if let Err(e) = self.client.delete(blob_id).await {
                log::error!("client error: {}", e);
            }
        }

        let parent_id = match self.inode_to_blobid.get(&parent).await {
            Some(parent_id) => parent_id,
            None => {
                log::error!("CREATE FAILED: EACCES");
                reply.error(EACCES);
                return;
            }
        };

        let str_name = name.to_string_lossy().to_string();

        let meta = Meta::new(&str_name, Type::File).with_parent(parent_id);

        let blob_id = match self.client.create_empty(meta.clone()).await {
            Ok(id) => id,
            Err(e) => {
                log::error!("client error: {}", e);
                reply.error(ENOENT);
                return;
            }
        };

        let ino = self.get_inode(&blob_id).await;
        self.inode_to_blobid.insert(ino, blob_id.clone()).await;
        self.name_to_blobid
            .insert((parent, str_name), blob_id)
            .await;

        reply.created(
            &constants::TTL,
            &build_attributes(ino, &meta, 0o764),
            0,
            0,
            _flags,
        )
    }

    async fn open(&self, _req: &Request, _ino: u64, flags: u32, reply: ReplyOpen) {
        let nd = flags as i32 & O_CREAT;
        log::info!("open {} [{}/{}]", _ino, flags, nd);
        if (flags as i32 & O_TRUNC) != 0 {
            log::info!("TRUNC REQUESTED");
        }
        reply.opened(0, flags);
    }

    async fn mknod(
        &self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        log::info!("MKNOD {}/{:?}", parent, name);

        let parent_id = match self.inode_to_blobid.get(&parent).await {
            Some(parent_id) => parent_id,
            None => {
                reply.error(EACCES);
                return;
            }
        };

        let str_name = name.to_string_lossy().to_string();

        let meta = Meta::new(&str_name, Type::File).with_parent(parent_id);

        let blob_id = match self.client.create_empty(meta.clone()).await {
            Ok(id) => id,
            Err(e) => {
                log::error!("client error: {}", e);
                reply.error(ENOENT);
                return;
            }
        };

        let ino = self.get_inode(&blob_id).await;
        self.inode_to_blobid.insert(ino, blob_id.clone()).await;
        self.name_to_blobid
            .insert((parent, str_name), blob_id)
            .await;

        reply.entry(&constants::TTL, &build_attributes(ino, &meta, 0o764), 0)
    }

    #[allow(clippy::too_many_arguments)]
    async fn write(
        &self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _flags: u32,
        reply: ReplyWrite,
    ) {
        log::info!("write {}bytes on {:?} @ {}", data.len(), ino, offset);

        let mut buffers_guard = self.write_buffers.lock().await;

        if let Some(mut buffer) = buffers_guard.remove(&ino) {
            if !buffer.write(offset as u64, data) {
                // Buffer isn't contiguous, we need to flush.
                let error_code = if let Some(blob_id) = self.inode_to_blobid.get(&ino).await {
                    if let Err(e) = self
                        .client
                        .write(&blob_id, buffer.offset, buffer.data.freeze())
                        .await
                    {
                        log::error!("write error: {}", e);
                        EACCES
                    } else {
                        buffers_guard.insert(ino, WriteBuffer::new(offset as u64, data));
                        reply.written(data.len() as u32);
                        return;
                    }
                } else {
                    ENOENT
                };

                reply.error(error_code);
            } else {
                buffers_guard.insert(ino, buffer);
                reply.written(data.len() as u32);
            }
        } else {
            buffers_guard.insert(ino, WriteBuffer::new(offset as u64, data));
            reply.written(data.len() as u32);
        }
    }

    async fn release(
        &self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let mut buffers_guard = self.write_buffers.lock().await;
        if let Some(buffer) = buffers_guard.remove(&ino) {
            log::info!("flushing pending write buffer for {}", ino);
            if let Some(blob_id) = self.inode_to_blobid.get(&ino).await {
                if let Err(e) = self
                    .client
                    .write(&blob_id, buffer.offset, buffer.data.freeze())
                    .await
                {
                    log::error!("write error: {}", e);
                    reply.error(EACCES);
                    return;
                }
                log::info!("flush complete");
            } else {
                reply.error(ENOENT);
                return;
            };
        }

        if let Some(blob_id) = self.inode_to_blobid.get(&ino).await {
            log::info!("calling fsync");
            if let Err(e) = self.client.fsync(&blob_id).await {
                log::error!("menmos fsync error: {}", e);
            }
            log::info!("fsync complete");
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    async fn readdir(
        &self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        log::info!("readdir on {:?}", ino);
        let entries_result = if let Some(v) = self.virtual_directories_inodes.get(&ino).await {
            self.list_virtual_entries(v, ino).await
        } else {
            // We assume the inode points to a directory blob id.
            let blob_id = match self.inode_to_blobid.get(&ino).await {
                Some(s) => s,
                None => {
                    reply.error(ENOENT);
                    return;
                }
            };
            self.list_entries(Query::default().and_parent(blob_id), ino)
                .await
        };

        match entries_result {
            Ok(entries) => {
                for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
                    //  TODO: Send the offset as the `from` param in the query instead.
                    // i + 1 means the index of the next entry
                    reply.add(entry.0, (i + 1) as i64, entry.1, entry.2);
                }
                reply.ok();
            }
            Err(e) => {
                log::error!("client error: {}", e);
                reply.error(ENOENT);
            }
        }
    }

    async fn unlink(&self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        log::info!("unlink {:?}/{:?}", parent, name);
        let str_name = name.to_string_lossy().to_string();

        let name_tuple = (parent, str_name);

        if let Some(blob_id) = self.name_to_blobid.get(&name_tuple).await {
            log::info!("DELETE {}", blob_id);
            if self.client.delete(blob_id.clone()).await.is_ok() {
                self.blobid_to_inode.remove(&blob_id).await;
            } else {
                reply.error(EACCES);
                return;
            }
        } else {
            reply.error(ENOENT);
            return;
        };

        self.name_to_blobid.remove(&name_tuple).await;
        reply.ok();
    }

    async fn rmdir(&self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let str_name = name.to_string_lossy().to_string();

        let error_code = if let Some(blob_id) = self.name_to_blobid.get(&(parent, str_name)).await {
            if self.rm_rf(&blob_id).await.is_ok() {
                reply.ok();
                return;
            } else {
                EACCES
            }
        } else {
            ENOENT
        };
        reply.error(error_code);
    }
}
