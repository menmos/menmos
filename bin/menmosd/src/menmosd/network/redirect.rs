use std::net::IpAddr;

use anyhow::Result;

use interface::StorageNodeInfo;

use ipnetwork::IpNetwork;

use warp::http::Uri;

use crate::{config::ServerSetting, Config};

fn should_redirect_local(
    request_ip: &IpAddr,
    public_address: &IpAddr,
    subnet_mask: &IpAddr,
) -> Result<bool> {
    Ok(request_ip == public_address
        || IpNetwork::with_netmask(*request_ip, *subnet_mask)?.prefix()
            == ipnetwork::ip_mask_to_prefix(*subnet_mask)?)
}

pub fn get_storage_node_address<S: AsRef<str>>(
    request_ip: IpAddr,
    n: StorageNodeInfo,
    cfg: &Config,
    path: S,
) -> Result<Uri> {
    let address = match n.redirect_info {
        interface::RedirectInfo::Automatic {
            public_address,
            local_address,
            subnet_mask,
        } => {
            let should_local = should_redirect_local(&request_ip, &public_address, &subnet_mask)?;
            if should_local {
                local_address
            } else {
                public_address
            }
        }
        interface::RedirectInfo::Static { static_address } => static_address,
    };

    let fmt_url = match &cfg.server {
        ServerSetting::Http(_http_setting) => {
            format!(
                "http://{}:{}/{}",
                address.to_string(),
                n.port,
                path.as_ref()
            )
        }
        ServerSetting::Https(https_setting) => {
            format!(
                "https://{}.{}:{}/{}",
                address.to_string().replace('.', "-"),
                https_setting.dns.root_domain,
                n.port,
                path.as_ref()
            )
        }
    };

    Ok(fmt_url.parse::<Uri>()?)
}
