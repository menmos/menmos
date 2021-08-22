use std::net::IpAddr;

use anyhow::{anyhow, Result};

use public_ip::{dns, http, BoxToResolver, ToResolver};

use crate::RedirectIp;

pub async fn get_redirect_info(
    subnet_mask: IpAddr,
    redirect_instructions: RedirectIp,
) -> Result<interface::RedirectInfo> {
    match redirect_instructions {
        RedirectIp::Automatic => {
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

            Ok(interface::RedirectInfo::Automatic {
                public_address,
                local_address,
                subnet_mask,
            })
        }
        RedirectIp::Static(static_redirect) => Ok(interface::RedirectInfo::Static {
            static_address: static_redirect,
        }),
    }
}
