use std::net::IpAddr;

use anyhow::{anyhow, Result};

use interface::StorageNodeInfo;

use public_ip::{dns, http, BoxToResolver, ToResolver};

use crate::RedirectIP;

pub async fn get_info<S: AsRef<str>>(
    port: u16,
    subnet_mask: IpAddr,
    name: S,
    redirect_instructions: RedirectIP,
) -> Result<StorageNodeInfo> {
    match redirect_instructions {
        RedirectIP::Automatic => {
            let resolver = vec![
                BoxToResolver::new(dns::OPENDNS_RESOLVER),
                BoxToResolver::new(http::HTTP_IPIFY_ORG_RESOLVER),
            ]
            .to_resolver();

            // Attempt to get an IP address and print it
            let public_address = public_ip::resolve_address(resolver)
                .await
                .ok_or_else(|| anyhow!("unable to get public address"))?;

            let local_address_str =
                local_ipaddress::get().ok_or_else(|| anyhow!("failed to get local IP"))?;
            let local_address = local_address_str.parse::<IpAddr>()?;

            Ok(StorageNodeInfo {
                id: name.as_ref().to_string(),
                redirect_info: interface::RedirectInfo::Automatic {
                    public_address,
                    local_address,
                    subnet_mask,
                },
                port,
            })
        }
        RedirectIP::Static(static_redirect) => Ok(StorageNodeInfo {
            id: name.as_ref().to_string(),
            redirect_info: interface::RedirectInfo::Static {
                static_address: static_redirect,
            },
            port,
        }),
    }
}
