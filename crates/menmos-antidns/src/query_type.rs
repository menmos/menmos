#[derive(PartialEq, Eq, Debug, Clone, Hash, Copy)]
#[allow(clippy::upper_case_acronyms)]
pub enum QueryType {
    UNKNOWN(u16),
    A,     // 1
    NS,    // 2
    CNAME, // 5
    MX,    // 15
    TXT,   // 16
    AAAA,  // 28
    CAA,   // 257
}

impl QueryType {
    pub fn to_num(self) -> u16 {
        match self {
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
