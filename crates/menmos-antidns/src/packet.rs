use snafu::{ResultExt, Snafu};

use crate::{
    header::HeaderError, question::QuestionError, record::RecordError, BytePacketBuffer, DnsHeader,
    DnsQuestion, DnsRecord, QueryType,
};

#[derive(Debug, Snafu)]
pub enum PacketError {
    InvalidHeader { source: HeaderError },
    InvalidQuestion { source: QuestionError },
    InvalidRecord { source: RecordError },
}

type Result<T> = std::result::Result<T, PacketError>;
#[derive(Clone, Debug)]
pub struct DnsPacket {
    pub header: DnsHeader,
    pub questions: Vec<DnsQuestion>,
    pub answers: Vec<DnsRecord>,
    pub authorities: Vec<DnsRecord>,
    pub resources: Vec<DnsRecord>,
}

impl DnsPacket {
    pub fn new() -> DnsPacket {
        DnsPacket {
            header: DnsHeader::new(),
            questions: Vec::new(),
            answers: Vec::new(),
            authorities: Vec::new(),
            resources: Vec::new(),
        }
    }

    pub fn from_buffer(buffer: &mut BytePacketBuffer) -> Result<DnsPacket> {
        log::trace!("deserializing packet from buffer");

        let mut result = DnsPacket::new();
        result.header.read(buffer).context(InvalidHeader)?;

        for _ in 0..result.header.questions {
            let mut question = DnsQuestion::new(String::new(), QueryType::UNKNOWN(0));
            question.read(buffer).context(InvalidQuestion)?;
            result.questions.push(question);
        }

        for i in 0..result.header.answers {
            log::trace!("deserializing answer {}", i);
            let rec = DnsRecord::read(buffer).context(InvalidRecord)?;
            result.answers.push(rec);
        }

        for _ in 0..result.header.authoritative_entries {
            let rec = DnsRecord::read(buffer).context(InvalidRecord)?;
            result.authorities.push(rec);
        }
        for _ in 0..result.header.resource_entries {
            let rec = DnsRecord::read(buffer).context(InvalidRecord)?;
            result.resources.push(rec);
        }

        Ok(result)
    }

    pub fn write(&mut self, buffer: &mut BytePacketBuffer) -> Result<()> {
        self.header.questions = self.questions.len() as u16;
        self.header.answers = self.answers.len() as u16;
        self.header.authoritative_entries = self.authorities.len() as u16;
        self.header.resource_entries = self.resources.len() as u16;

        self.header.write(buffer).context(InvalidHeader)?;

        for question in &self.questions {
            question.write(buffer).context(InvalidQuestion)?;
        }
        for rec in &self.answers {
            rec.write(buffer).context(InvalidRecord)?;
        }
        for rec in &self.authorities {
            rec.write(buffer).context(InvalidRecord)?;
        }
        for rec in &self.resources {
            rec.write(buffer).context(InvalidRecord)?;
        }

        Ok(())
    }
}
