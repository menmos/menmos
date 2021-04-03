use std::net::IpAddr;

use snafu::Snafu;

use crate::extract;
use crate::{Config, DnsPacket, DnsRecord, QueryType};

#[derive(Debug, Snafu)]
pub enum ResolveError {
    UnexpectedIpv6,
}

type Result<T> = std::result::Result<T, ResolveError>;

fn in_house_resolution(qname: &str, ip: IpAddr) -> Result<DnsPacket> {
    log::info!("resolving {} in-house to {}", qname, ip);

    match ip {
        IpAddr::V4(ip) => {
            let mut pkt = DnsPacket::new();
            pkt.header.id = 6666;
            pkt.header.questions = 0;
            pkt.header.recursion_desired = false;
            pkt.header.response = true;
            pkt.header.authoritative_answer = true;
            pkt.answers.push(DnsRecord::A {
                domain: qname.to_string(),
                addr: ip,
                ttl: 60,
            });

            Ok(pkt)
        }
        IpAddr::V6(_) => Err(ResolveError::UnexpectedIpv6),
    }
}

pub async fn lookup(qname: &str, qtype: QueryType, cfg: &Config) -> Result<Option<DnsPacket>> {
    log::debug!("lookup [{:?}] on {}", qtype, qname);
    // Attempt to forward A queries that have a serialized IP in their domain to the IP itself.
    if qtype == QueryType::A {
        if let Ok(ip) = extract::ip_address_from_url(qname) {
            return Ok(Some(in_house_resolution(qname, IpAddr::V4(ip))?));
        } else if qname == cfg.host_name {
            return Ok(Some(in_house_resolution(qname, cfg.public_ip)?));
        }
    }

    if qtype == QueryType::CAA {
        // Here we do a very bad thing and don't really check or allow the possibility for CAA records.
        // This means that any public CA has the authority to emit certificates for your domain.
        // If you really _need_ CAA, you should probably be using something more serious than this
        // embedded DNS server.
        log::debug!("skipping resolution of a CAA query");
        return Ok(Some(DnsPacket::new()));
    }
    if qtype == QueryType::AAAA && qname.ends_with(&cfg.root_domain) {
        log::debug!("returning blank AAAA response");
        return Ok(Some(DnsPacket::new()));
    }

    // Otherwise we ditch the packet.
    Ok(None)
}
