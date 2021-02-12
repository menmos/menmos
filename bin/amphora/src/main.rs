mod logging;

use std::time::Duration;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;

use clap::Clap;

use interface::message::directory_node::CertificateInfo;
use interface::StorageNode;

use amphora::{constants, make_node, CertPath, Config, Server, Storage};

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn load_certificates<P: AsRef<Path>>(cert_directory: P) -> Option<CertificateInfo> {
    CertificateInfo::from_path(
        cert_directory
            .as_ref()
            .join(constants::CERTIFICATE_FILE_NAME),
        cert_directory
            .as_ref()
            .join(constants::PRIVATE_KEY_FILE_NAME),
    )
    .ok()
}

async fn block_until_cert_change(n: Arc<Storage>, initial_certificates: Option<CertificateInfo>) {
    loop {
        let new_certs_maybe = n.get_certificates().await;

        if initial_certificates != new_certs_maybe {
            break;
        }

        tokio::time::delay_for(Duration::from_secs(30)).await;
    }
}

#[tokio::main]
async fn main_loop(cfg_path: &Option<PathBuf>) -> Result<()> {
    let cfg = Config::new(cfg_path)?;
    logging::init_logger(&cfg.log_config_file)?;

    loop {
        let certs = load_certificates(&cfg.server.certificate_storage_path);

        let storage_node = Arc::from(make_node(cfg.clone(), certs.clone()).await?);

        // Start the periodic registration task.
        let (registration_handle, mut registration_stop) = {
            let node_cloned = storage_node.clone();
            let (stop_tx, mut stop_rx) = tokio::sync::mpsc::channel(1);
            let job_handle = tokio::task::spawn(async move {
                loop {
                    match node_cloned.update_registration().await {
                        Ok(_) => {
                            log::info!("directory registration complete")
                        }
                        Err(e) => {
                            log::error!("failed to update registration: {}", e)
                        }
                    }

                    let stop_future = stop_rx.recv();
                    let delay = tokio::time::delay_for(Duration::from_secs(20));

                    let should_stop = tokio::select! {
                        _ = delay => {
                            false
                        }
                        _ = stop_future => {
                            true
                        }
                    };

                    if should_stop {
                        break;
                    }
                }
            });
            (job_handle, stop_tx)
        };

        // Start the server.
        let cert_paths = if certs.is_some() {
            Some(CertPath {
                certificate: cfg
                    .server
                    .certificate_storage_path
                    .join(constants::CERTIFICATE_FILE_NAME),
                private_key: cfg
                    .server
                    .certificate_storage_path
                    .join(constants::PRIVATE_KEY_FILE_NAME),
            })
        } else {
            None
        };

        let s = Server::new(storage_node.clone(), cfg.clone(), cert_paths);

        let cert_change_validator = block_until_cert_change(storage_node, certs);
        let ctrl_c_signal = tokio::signal::ctrl_c();

        let should_terminate = tokio::select! {
            _ = cert_change_validator => {
                false
            }
            _ = ctrl_c_signal => {
                log::info!("received SIGINT");
                true
            }
        };

        s.stop().await?;

        registration_stop.send(()).await?;
        registration_handle.await?;

        if should_terminate {
            break;
        }
    }

    Ok(())
}

#[derive(Clap, Debug)]
#[clap(version = VERSION)]
pub struct CLIMain {
    #[clap(short = 'c', long = "cfg")]
    cfg: Option<PathBuf>,
}

impl CLIMain {
    pub fn run(self) -> Result<()> {
        main_loop(&self.cfg)
    }
}

fn main() {
    let cli = CLIMain::parse();
    if let Err(e) = cli.run() {
        log::error!("fatal: {}", e);
    }
}
