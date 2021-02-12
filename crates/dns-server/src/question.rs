use snafu::{ResultExt, Snafu};

use crate::{packet_buffer::BufferError, BytePacketBuffer, QueryType};

#[derive(Debug, Snafu)]
pub enum QuestionError {
    InvalidBuffer { source: BufferError },
}

type Result<T> = std::result::Result<T, QuestionError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsQuestion {
    pub name: String,
    pub qtype: QueryType,
}

impl DnsQuestion {
    pub fn new(name: String, qtype: QueryType) -> DnsQuestion {
        DnsQuestion { name, qtype }
    }

    pub fn read(&mut self, buffer: &mut BytePacketBuffer) -> Result<()> {
        buffer.read_qname(&mut self.name).context(InvalidBuffer)?;
        self.qtype = QueryType::from_num(buffer.read_u16().context(InvalidBuffer)?); // qtype
        let _ = buffer.read_u16().context(InvalidBuffer)?; // class

        Ok(())
    }

    pub fn write(&self, buffer: &mut BytePacketBuffer) -> Result<()> {
        buffer.write_qname(&self.name).context(InvalidBuffer)?;

        let typenum = self.qtype.to_num();
        buffer.write_u16(typenum).context(InvalidBuffer)?;
        buffer.write_u16(1).context(InvalidBuffer)?;

        Ok(())
    }
}
