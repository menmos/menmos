//! Library for spawning pre-configured processes.

mod daemon;
mod logging;

pub use daemon::{Daemon, DaemonProcess};
