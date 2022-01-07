use std::net::{Ipv4Addr, Ipv6Addr};

use snafu::prelude::*;

use crate::{packet_buffer::BufferError, BytePacketBuffer, QueryType};

#[derive(Debug, Snafu)]
pub enum RecordError {
    InvalidBuffer { source: BufferError },
    StringTooLong,
}

type Result<T> = std::result::Result<T, RecordError>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[allow(dead_code)]
#[allow(clippy::upper_case_acronyms)] // We allow for this because DNS queries are always written uppercase.
pub enum DnsRecord {
    #[allow(clippy::upper_case_acronyms)]
    UNKNOWN {
        domain: String,
        qtype: u16,
        data_len: u16,
        ttl: u32,
    }, // 0
    #[allow(clippy::upper_case_acronyms)]
    A {
        domain: String,
        addr: Ipv4Addr,
        ttl: u32,
    }, // 1
    #[allow(clippy::upper_case_acronyms)]
    NS {
        domain: String,
        host: String,
        ttl: u32,
    }, // 2
    #[allow(clippy::upper_case_acronyms)]
    CNAME {
        domain: String,
        host: String,
        ttl: u32,
    }, // 5
    #[allow(clippy::upper_case_acronyms)]
    MX {
        domain: String,
        priority: u16,
        host: String,
        ttl: u32,
    }, // 15
    #[allow(clippy::upper_case_acronyms)]
    TXT {
        domain_bytes: Vec<u8>, // usually 2 bytes (jump).
        // Qtype => 2 bytes
        // Unknown number => 2 bytes
        ttl: u32,      // 4 bytes
        data_len: u16, // 2 bytes
        text: Vec<Vec<u8>>,
    }, // 16
    #[allow(clippy::upper_case_acronyms)]
    AAAA {
        domain: String,
        addr: Ipv6Addr,
        ttl: u32,
    }, // 28
    #[allow(clippy::upper_case_acronyms)]
    CAA {},
}

impl DnsRecord {
    pub fn read(buffer: &mut BytePacketBuffer) -> Result<DnsRecord> {
        let mut domain = String::new();
        let idx_before = buffer.pos();
        buffer.read_qname(&mut domain).context(InvalidBufferSnafu)?;
        let idx_after = buffer.pos();
        let domain_bytes = buffer.buf[idx_before..idx_after].to_vec();

        let qtype_num = buffer.read_u16().context(InvalidBufferSnafu)?;
        let qtype = QueryType::from_num(qtype_num);
        let _ = buffer.read_u16().context(InvalidBufferSnafu)?;
        let ttl = buffer.read_u32().context(InvalidBufferSnafu)?;
        let data_len = buffer.read_u16().context(InvalidBufferSnafu)?;

        match qtype {
            QueryType::A => {
                let raw_addr = buffer.read_u32().context(InvalidBufferSnafu)?;
                let addr = Ipv4Addr::new(
                    ((raw_addr >> 24) & 0xFF) as u8,
                    ((raw_addr >> 16) & 0xFF) as u8,
                    ((raw_addr >> 8) & 0xFF) as u8,
                    (raw_addr & 0xFF) as u8,
                );

                Ok(DnsRecord::A { domain, addr, ttl })
            }
            QueryType::AAAA => {
                let raw_addr1 = buffer.read_u32().context(InvalidBufferSnafu)?;
                let raw_addr2 = buffer.read_u32().context(InvalidBufferSnafu)?;
                let raw_addr3 = buffer.read_u32().context(InvalidBufferSnafu)?;
                let raw_addr4 = buffer.read_u32().context(InvalidBufferSnafu)?;
                let addr = Ipv6Addr::new(
                    ((raw_addr1 >> 16) & 0xFFFF) as u16,
                    (raw_addr1 & 0xFFFF) as u16,
                    ((raw_addr2 >> 16) & 0xFFFF) as u16,
                    (raw_addr2 & 0xFFFF) as u16,
                    ((raw_addr3 >> 16) & 0xFFFF) as u16,
                    (raw_addr3 & 0xFFFF) as u16,
                    ((raw_addr4 >> 16) & 0xFFFF) as u16,
                    (raw_addr4 & 0xFFFF) as u16,
                );

                Ok(DnsRecord::AAAA { domain, addr, ttl })
            }
            QueryType::NS => {
                let mut ns = String::new();
                buffer.read_qname(&mut ns).context(InvalidBufferSnafu)?;

                Ok(DnsRecord::NS {
                    domain,
                    host: ns,
                    ttl,
                })
            }
            QueryType::CNAME => {
                let mut cname = String::new();
                buffer.read_qname(&mut cname).context(InvalidBufferSnafu)?;

                Ok(DnsRecord::CNAME {
                    domain,
                    host: cname,
                    ttl,
                })
            }
            QueryType::MX => {
                let priority = buffer.read_u16().context(InvalidBufferSnafu)?;
                let mut mx = String::new();
                buffer.read_qname(&mut mx).context(InvalidBufferSnafu)?;

                Ok(DnsRecord::MX {
                    domain,
                    priority,
                    host: mx,
                    ttl,
                })
            }
            QueryType::TXT => {
                tracing::debug!("deserializing TXT record");

                let mut text = Vec::new();
                loop {
                    if buffer.pos() == 512 {
                        tracing::trace!("reached EOF -> TXT record finished");
                        break;
                    }

                    let string_length = buffer.read().context(InvalidBufferSnafu)?;
                    if string_length == 0 {
                        tracing::trace!("got null terminator -> TXT record finished");
                        break;
                    }

                    {
                        let string_bytes = buffer
                            .get_range(buffer.pos(), string_length as usize)
                            .context(InvalidBufferSnafu)?;

                        tracing::debug!(
                            "got TXT string: '{}'",
                            String::from_utf8_lossy(string_bytes)
                        );

                        text.push(string_bytes.to_vec());
                    }
                    buffer
                        .step(string_length as usize)
                        .context(InvalidBufferSnafu)?;
                }
                Ok(DnsRecord::TXT {
                    domain_bytes,
                    ttl,
                    data_len,
                    text,
                })
            }
            QueryType::CAA => Ok(DnsRecord::CAA {}), // TODO: CAA is utterly unimplemented because we don't need it. Would be nice to have it though.
            QueryType::UNKNOWN(_) => {
                buffer.step(data_len as usize).context(InvalidBufferSnafu)?;

                Ok(DnsRecord::UNKNOWN {
                    domain,
                    qtype: qtype_num,
                    data_len,
                    ttl,
                })
            }
        }
    }

    pub fn write(&self, buffer: &mut BytePacketBuffer) -> Result<usize> {
        let start_pos = buffer.pos();

        match *self {
            DnsRecord::A {
                ref domain,
                ref addr,
                ttl,
            } => {
                buffer.write_qname(domain).context(InvalidBufferSnafu)?;
                buffer
                    .write_u16(QueryType::A.to_num())
                    .context(InvalidBufferSnafu)?;
                buffer.write_u16(1).context(InvalidBufferSnafu)?;
                buffer.write_u32(ttl).context(InvalidBufferSnafu)?;
                buffer.write_u16(4).context(InvalidBufferSnafu)?;

                let octets = addr.octets();
                buffer.write_u8(octets[0]).context(InvalidBufferSnafu)?;
                buffer.write_u8(octets[1]).context(InvalidBufferSnafu)?;
                buffer.write_u8(octets[2]).context(InvalidBufferSnafu)?;
                buffer.write_u8(octets[3]).context(InvalidBufferSnafu)?;
            }
            DnsRecord::NS {
                ref domain,
                ref host,
                ttl,
            } => {
                buffer.write_qname(domain).context(InvalidBufferSnafu)?;
                buffer
                    .write_u16(QueryType::NS.to_num())
                    .context(InvalidBufferSnafu)?;
                buffer.write_u16(1).context(InvalidBufferSnafu)?;
                buffer.write_u32(ttl).context(InvalidBufferSnafu)?;

                let pos = buffer.pos();
                buffer.write_u16(0).context(InvalidBufferSnafu)?;

                buffer.write_qname(host).context(InvalidBufferSnafu)?;

                let size = buffer.pos() - (pos + 2);
                buffer
                    .set_u16(pos, size as u16)
                    .context(InvalidBufferSnafu)?;
            }
            DnsRecord::CNAME {
                ref domain,
                ref host,
                ttl,
            } => {
                buffer.write_qname(domain).context(InvalidBufferSnafu)?;
                buffer
                    .write_u16(QueryType::CNAME.to_num())
                    .context(InvalidBufferSnafu)?;
                buffer.write_u16(1).context(InvalidBufferSnafu)?;
                buffer.write_u32(ttl).context(InvalidBufferSnafu)?;

                let pos = buffer.pos();
                buffer.write_u16(0).context(InvalidBufferSnafu)?;

                buffer.write_qname(host).context(InvalidBufferSnafu)?;

                let size = buffer.pos() - (pos + 2);
                buffer
                    .set_u16(pos, size as u16)
                    .context(InvalidBufferSnafu)?;
            }
            DnsRecord::MX {
                ref domain,
                priority,
                ref host,
                ttl,
            } => {
                buffer.write_qname(domain).context(InvalidBufferSnafu)?;
                buffer
                    .write_u16(QueryType::MX.to_num())
                    .context(InvalidBufferSnafu)?;
                buffer.write_u16(1).context(InvalidBufferSnafu)?;
                buffer.write_u32(ttl).context(InvalidBufferSnafu)?;

                let pos = buffer.pos();
                buffer.write_u16(0).context(InvalidBufferSnafu)?;

                buffer.write_u16(priority).context(InvalidBufferSnafu)?;
                buffer.write_qname(host).context(InvalidBufferSnafu)?;

                let size = buffer.pos() - (pos + 2);
                buffer
                    .set_u16(pos, size as u16)
                    .context(InvalidBufferSnafu)?;
            }
            DnsRecord::TXT {
                ref domain_bytes,
                ttl,
                data_len,
                ref text,
            } => {
                tracing::debug!("serializing TXT record");
                buffer
                    .write_bytes(domain_bytes.as_ref())
                    .context(InvalidBufferSnafu)?;
                buffer
                    .step(domain_bytes.len())
                    .context(InvalidBufferSnafu)?;
                buffer
                    .write_u16(QueryType::TXT.to_num())
                    .context(InvalidBufferSnafu)?;
                buffer.write_u16(1).context(InvalidBufferSnafu)?;
                buffer.write_u32(ttl).context(InvalidBufferSnafu)?;
                buffer.write_u16(data_len + 1).context(InvalidBufferSnafu)?;

                for string in text.iter() {
                    ensure!(string.len() <= 255, StringTooLongSnafu);

                    tracing::trace!(
                        "writing string '{}' with length {}",
                        String::from_utf8_lossy(string.as_ref()),
                        string.len()
                    );
                    buffer
                        .write(string.len() as u8)
                        .context(InvalidBufferSnafu)?;
                    buffer
                        .write_bytes(string.as_ref())
                        .context(InvalidBufferSnafu)?;
                    buffer
                        .step(string.len() as usize)
                        .context(InvalidBufferSnafu)?;
                }
            }
            DnsRecord::AAAA {
                ref domain,
                ref addr,
                ttl,
            } => {
                buffer.write_qname(domain).context(InvalidBufferSnafu)?;
                buffer
                    .write_u16(QueryType::AAAA.to_num())
                    .context(InvalidBufferSnafu)?;
                buffer.write_u16(1).context(InvalidBufferSnafu)?;
                buffer.write_u32(ttl).context(InvalidBufferSnafu)?;
                buffer.write_u16(16).context(InvalidBufferSnafu)?;

                for octet in &addr.segments() {
                    buffer.write_u16(*octet).context(InvalidBufferSnafu)?;
                }
            }
            DnsRecord::CAA {} => {
                tracing::debug!("writing nothing instead of CAA record");
            }
            DnsRecord::UNKNOWN { .. } => {
                tracing::warn!("skipping record: {:?}", self);
            }
        }

        Ok(buffer.pos() - start_pos)
    }
}
