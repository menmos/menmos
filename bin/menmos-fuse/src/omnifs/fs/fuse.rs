use std::{ffi::OsStr, time::SystemTime};

use async_fuse::{
    Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyWrite, Request,
};

use async_trait::async_trait;

use libc::{EACCES, ENOENT, O_CREAT, O_TRUNC};
use menmos_client::{Meta, Query, Type};

use crate::constants;
use crate::WriteBuffer;

use super::OmniFS;

#[async_trait]
impl Filesystem for OmniFS {
    async fn lookup(&self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        match self.lookup_impl(parent, name).await {
            Ok(resp) => {
                reply.entry(&resp.ttl, &resp.attrs, resp.generation);
            }
            Err(e) => reply.error(e.to_error_code()),
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
        match self.mkdir_impl(parent, name).await {
            Ok(resp) => {
                reply.entry(&resp.ttl, &resp.attrs, resp.generation);
            }
            Err(e) => reply.error(e.to_error_code()),
        }
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

        match self.rename_impl(parent, name, newparent, newname).await {
            Ok(_) => reply.ok(),
            Err(e) => reply.error(e.to_error_code()),
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
        match self.getattr_impl(ino).await {
            Ok(resp) => reply.attr(&resp.ttl, &resp.attrs),
            Err(e) => reply.error(e.to_error_code()),
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
        match self.read_impl(ino, offset, size).await {
            Ok(resp) => reply.data(&resp.data),
            Err(e) => reply.error(e.to_error_code()),
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
        match self.create_impl(parent, name).await {
            Ok(resp) => reply.created(
                &resp.ttl,
                &resp.attrs,
                resp.generation,
                resp.file_handle,
                _flags,
            ),
            Err(e) => reply.error(e.to_error_code()),
        }
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
