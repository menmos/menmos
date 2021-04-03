#[derive(PartialEq, Eq, Debug, Clone, Hash, Copy)]
pub enum QueryType {
    #[allow(clippy::upper_case_acronyms)]
    UNKNOWN(u16),
    #[allow(clippy::upper_case_acronyms)]
    A, // 1
    #[allow(clippy::upper_case_acronyms)]
    NS, // 2
    #[allow(clippy::upper_case_acronyms)]
    CNAME, // 5
    #[allow(clippy::upper_case_acronyms)]
    MX, // 15
    #[allow(clippy::upper_case_acronyms)]
    TXT, // 16
    #[allow(clippy::upper_case_acronyms)]
    AAAA, // 28
    #[allow(clippy::upper_case_acronyms)]
    CAA, // 257
}

impl QueryType {
    pub fn to_num(&self) -> u16 {
        match *self {
            QueryType::UNKNOWN(x) => x,
            QueryType::A => 1,
            QueryType::NS => 2,
            QueryType::CNAME => 5,
            QueryType::MX => 15,
            QueryType::TXT => 16,
            QueryType::AAAA => 28,
            QueryType::CAA => 257,
        }
    }

    pub fn from_num(num: u16) -> QueryType {
        match num {
            1 => QueryType::A,
            2 => QueryType::NS,
            5 => QueryType::CNAME,
            15 => QueryType::MX,
            16 => QueryType::TXT,
            28 => QueryType::AAAA,
            257 => QueryType::CAA,
            _ => QueryType::UNKNOWN(num),
        }
    }
}
