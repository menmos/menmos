use std::fs;
use std::path::Path;

use anyhow::Result;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MessageResponse {
    pub message: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VersionResponse {
    pub version: String,
}

pub mod directory_node {

    use std::convert::TryFrom;

    use rapidquery::Expression;

    pub use super::*;

    fn default_from() -> usize {
        0
    }

    fn default_size() -> usize {
        30
    }

    fn default_sign_urls() -> bool {
        true
    }

    fn default_facets() -> bool {
        false
    }

    fn file_to_base64<P: AsRef<Path>>(path: P) -> Result<String> {
        Ok(base64::encode(fs::read(path.as_ref())?))
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    pub struct CertificateInfo {
        pub certificate_b64: String,
        pub private_key_b64: String,
    }

    impl CertificateInfo {
        pub fn from_path<P: AsRef<Path>, Q: AsRef<Path>>(
            certificate_path: P,
            private_key_path: Q,
        ) -> Result<CertificateInfo> {
            Ok(Self {
                certificate_b64: file_to_base64(certificate_path)?,
                private_key_b64: file_to_base64(private_key_path)?,
            })
        }
    }

    #[derive(Clone, Debug, Deserialize, Serialize)]
    #[serde(deny_unknown_fields)]
    pub struct RegisterResponse {
        pub message: String,
        pub certificates: Option<CertificateInfo>,
        pub rebuild_requested: bool,
    }

    #[derive(Clone, Debug, Deserialize, Hash, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    #[serde(untagged)]
    pub enum ExpressionRequest {
        Expr(Expression),
        Raw(String),
    }

    impl Default for ExpressionRequest {
        fn default() -> Self {
            ExpressionRequest::Expr(Expression::Empty)
        }
    }

    #[derive(Clone, Debug, Deserialize, Hash, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct QueryRequest {
        #[serde(default = "ExpressionRequest::default")]
        pub expression: ExpressionRequest,

        #[serde(default = "default_from")]
        pub from: usize,

        #[serde(default = "default_size")]
        pub size: usize,

        #[serde(default = "default_sign_urls")]
        pub sign_urls: bool,

        #[serde(default = "default_facets")]
        pub facets: bool, // TODO: Permit requesting facets for specific tags instead of doing it for all.
    }

    #[derive(Clone, Debug, Deserialize, Hash, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct Query {
        #[serde(default = "Expression::default")]
        pub expression: Expression,

        #[serde(default = "default_from")]
        pub from: usize,

        #[serde(default = "default_size")]
        pub size: usize,

        #[serde(default = "default_sign_urls")]
        pub sign_urls: bool,

        #[serde(default = "default_facets")]
        pub facets: bool, // TODO: Permit requesting facets for specific tags instead of doing it for all.
    }

    impl TryFrom<QueryRequest> for Query {
        type Error = anyhow::Error;

        fn try_from(request: QueryRequest) -> Result<Self, Self::Error> {
            let expression = match request.expression {
                ExpressionRequest::Expr(e) => e,
                ExpressionRequest::Raw(raw) => Expression::parse(raw)?,
            };

            Ok(Query {
                expression,
                from: request.from,
                size: request.size,
                sign_urls: request.sign_urls,
                facets: request.facets,
            })
        }
    }

    impl Query {
        pub fn with_expression<S: Into<String>>(mut self, expression: S) -> Result<Self> {
            self.expression = Expression::parse(expression.into())?;
            Ok(self)
        }

        pub fn and_tag<S: Into<String>>(mut self, tag: S) -> Self {
            let new_expr = Expression::Tag { tag: tag.into() };
            self.expression = Expression::And {
                and: (Box::from(self.expression), Box::from(new_expr)),
            };
            self
        }

        pub fn and_meta<K: Into<String>, V: Into<String>>(mut self, k: K, v: V) -> Self {
            let new_expr = Expression::KeyValue {
                key: k.into(),
                value: v.into(),
            };
            self.expression = Expression::And {
                and: (Box::from(self.expression), Box::from(new_expr)),
            };
            self
        }

        pub fn and_parent<P: Into<String>>(mut self, p: P) -> Self {
            let new_expr = Expression::Parent { parent: p.into() };
            self.expression = Expression::And {
                and: (Box::from(self.expression), Box::from(new_expr)),
            };
            self
        }

        pub fn with_from(mut self, f: usize) -> Self {
            self.from = f;
            self
        }

        pub fn with_size(mut self, s: usize) -> Self {
            self.size = s;
            self
        }

        pub fn with_facets(mut self, f: bool) -> Self {
            self.facets = f;
            self
        }
    }

    impl Default for Query {
        fn default() -> Self {
            Query {
                expression: Default::default(),
                from: default_from(),
                size: default_size(),
                sign_urls: default_sign_urls(),
                facets: default_facets(),
            }
        }
    }
}

pub mod storage_node {
    pub use super::*;

    #[derive(Clone, Debug, Deserialize, Serialize)]
    #[serde(deny_unknown_fields)]
    pub struct PutResponse {
        pub id: String,
    }
}
