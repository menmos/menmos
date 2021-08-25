//! Library for spawning pre-configured processes.

mod daemon;
pub mod logging;

pub use daemon::{Daemon, DaemonProcess};
