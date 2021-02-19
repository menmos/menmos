use std::time::{Duration, UNIX_EPOCH};

use async_fuse::{FileAttr, FileType};

pub const BLOCK_SIZE: u64 = 512_1000; // 512kb
pub const TTL: Duration = Duration::from_secs(1);

pub const ROOT_DIR_ATTR: FileAttr = FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: UNIX_EPOCH, // 1970-01-01 00:00:00
    mtime: UNIX_EPOCH,
    ctime: UNIX_EPOCH,
    crtime: UNIX_EPOCH,
    kind: FileType::Directory,
    perm: 0o755,
    nlink: 2,
    uid: 1000,
    gid: 1000,
    rdev: 0,
    flags: 0,
};
