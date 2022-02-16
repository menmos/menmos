mod builder;
mod client;
mod parameters;

pub use builder::BuildError;
use builder::ClientBuilder;

use parameters::Parameters;

pub use client::{Client, ClientError};
pub use interface::BlobMetaRequest as Meta;
pub use interface::{Expression, Query, QueryResponse};
