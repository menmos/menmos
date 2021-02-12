mod data;
mod sign;
mod validate;

use data::TokenData;

pub use sign::sign;
pub use validate::validate;

const TOKEN_TTL_SECONDS: u32 = 60 * 60 * 6; // 6 hours.
