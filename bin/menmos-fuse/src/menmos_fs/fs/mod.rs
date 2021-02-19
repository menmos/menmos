mod common;
mod create;
mod error;
mod getattr;
mod lookup;
mod mkdir;
mod mknod;
mod read;
mod readdir;
mod release;
mod rename;
mod rmdir;
mod unlink;
mod write;

mod virtualdir;

pub use common::MenmosFS;
pub use error::{Error, Result};

use crate::constants;
use async_fuse::{FileAttr, FileType};
use menmos_client::{Meta, Type};
use std::time::UNIX_EPOCH;

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
