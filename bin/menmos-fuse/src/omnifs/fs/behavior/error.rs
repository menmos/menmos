use libc::c_int;
use libc::{EACCES, EIO, ENOENT};
pub enum Error {
    NotFound,
    Forbidden,
    IOError,
}

impl Error {
    pub fn to_error_code(&self) -> c_int {
        match self {
            Error::NotFound => ENOENT,
            Error::Forbidden => EACCES,
            Error::IOError => EIO,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
