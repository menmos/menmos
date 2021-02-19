use std::fmt;

use libc::c_int;
use libc::{EACCES, EIO, ENOENT};

#[derive(Debug)]
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

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;
