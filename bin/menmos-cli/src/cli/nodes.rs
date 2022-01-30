use anyhow::Result;
use clap::Parser;
use menmos::Menmos;
use rood::cli::OutputManager;

#[derive(Parser)]
pub struct ListStorageNodesCommand {}

impl ListStorageNodesCommand {
    pub async fn run(self, cli: OutputManager, client: Menmos) -> Result<()> {
        cli.step("Storage Nodes");

        let storage_nodes = client.client().list_storage_nodes().await?;

        let pushed = cli.push();

        for node in storage_nodes.storage_nodes {
            pushed.step(node.id);

            let pushed = pushed.push();
            pushed.step(format!("Port: {}", node.port));
            pushed.step(format!(
                "Storage Used: {:.2}/{} Gb",
                node.size as f32 / (1024.0 * 1024.0 * 1024.0),
                (node.available_space + node.size) / (1024 * 1024 * 1024)
            ));
            pushed.step("Redirect Info");
            let pushed = pushed.push();

            match node.redirect_info {
                menmos::interface::RedirectInfo::Automatic {
                    public_address,
                    local_address,
                    subnet_mask,
                } => {
                    pushed.step(format!("Public Address: {}", public_address));
                    pushed.step(format!("Local Address: {}", local_address));
                    pushed.step(format!("Subnet Mask: {}", subnet_mask));
                }

                menmos::interface::RedirectInfo::Static { static_address } => {
                    pushed.step(format!("Static Address: {}", static_address));
                }
            }
        }

        Ok(())
    }
}
