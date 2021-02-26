use menmosd::Daemon;

const BINARY_NAME: &str = env!("CARGO_PKG_NAME");

fn main() {
    xecute::DaemonProcess::start(BINARY_NAME, "Menmos Master Server", Daemon::new())
}
