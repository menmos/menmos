//! Library for spawning pre-configured processes.

mod daemon;
pub mod logging;
mod telemetry;

pub use daemon::{Daemon, DaemonProcess};
