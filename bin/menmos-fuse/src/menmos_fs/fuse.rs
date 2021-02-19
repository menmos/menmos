use std::{ffi::OsStr, time::SystemTime};

use async_fuse::{
    Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyWrite, Request,
};

use async_trait::async_trait;

use super::MenmosFS;

#[async_trait]
impl Filesystem for MenmosFS {
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
        match self.mknod_impl(parent, name).await {
            Ok(resp) => reply.entry(&resp.ttl, &resp.attrs, resp.generation),
            Err(e) => reply.error(e.to_error_code()),
        }
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
        match self.write_impl(ino, offset, data).await {
            Ok(resp) => reply.written(resp.written),
            Err(e) => reply.error(e.to_error_code()),
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
        match self.release_impl(ino).await {
            Ok(_) => reply.ok(),
            Err(e) => reply.error(e.to_error_code()),
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
        match self.readdir_impl(ino, offset).await {
            Ok(resp) => {
                for entry in resp.entries.into_iter() {
                    reply.add(entry.ino, entry.offset, entry.kind, &entry.name);
                }
                reply.ok()
            }
            Err(e) => reply.error(e.to_error_code()),
        }
    }

    async fn unlink(&self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self.unlink_impl(parent, name).await {
            Ok(_) => reply.ok(),
            Err(e) => reply.error(e.to_error_code()),
        }
    }

    async fn rmdir(&self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self.rmdir_impl(parent, name).await {
            Ok(_) => reply.ok(),
            Err(e) => reply.error(e.to_error_code()),
        }
    }
}
