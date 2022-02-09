use std::convert::TryFrom;

use anyhow::anyhow;

use interface::{BlobMeta, CertificateInfo, Query, StorageNodeInfo};

use rapidquery::Expression;

use serde::{Deserialize, Serialize};

pub mod blobmeta {
    use super::*;

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    pub struct ListMetadataRequest {
        /// Optionally filter which tags to return (defaults to all).
        pub tags: Option<Vec<String>>,

        /// Optionally filter which keys to return (defaults to all). [e.g. "filetype"]
        pub meta_keys: Option<Vec<String>>,
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    pub struct GetMetaResponse {
        pub meta: Option<BlobMeta>,
    }
}

pub mod routing {
    use interface::RoutingConfig;

    use super::*;

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    pub struct GetRoutingConfigResponse {
        pub routing_config: Option<RoutingConfig>,
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    pub struct SetRoutingConfigRequest {
        pub routing_config: RoutingConfig,
    }
}

pub mod auth {
    use super::*;

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    pub struct LoginRequest {
        pub username: String,
        pub password: String,
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    pub struct LoginResponse {
        pub token: String,
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    pub struct RegisterRequest {
        pub username: String,
        pub password: String,
    }
}

pub mod storage {
    use super::*;

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    pub struct ListStorageNodesResponse {
        pub storage_nodes: Vec<StorageNodeInfo>,
    }

    #[derive(Clone, Debug, Deserialize, Serialize)]
    #[serde(deny_unknown_fields)]
    pub struct MoveRequest {
        pub blob_id: String,
        pub destination_url: String,
        pub owner_username: String,
    }

    #[derive(Clone, Debug, Deserialize, Serialize)]
    #[serde(deny_unknown_fields)]
    pub struct RegisterResponse {
        pub message: String,
        pub certificates: Option<CertificateInfo>,
        pub rebuild_requested: bool,
        pub move_requests: Vec<MoveRequest>,
    }
}

pub mod query {
    use interface::{ExpressionField, SortOrder};

    use super::*;

    #[derive(Clone, Debug, Deserialize, Hash, PartialEq, Eq, Serialize)]
    #[serde(untagged)]
    pub enum RawExpression {
        /// Tag expression.
        ///
        /// Evaluates to resolver items where the tag is present.
        Tag { tag: String },
        /// Key/Value expression.
        ///
        /// Evaluates to resolver items where the given field/value pair is present.
        Field { key: String, value: String },
        /// HasKey expression.
        ///
        /// Evaluates to resolver items where the given key is present (in any key/value pair).
        HasKey { key: String },
        /// And expression.
        ///
        /// Evaluates to the intersection of its two sub-expressions.
        And {
            and: (Box<RawExpression>, Box<RawExpression>),
        },
        /// Or expression.
        /// Evaluates to the union of its two sub-expressions.
        Or {
            or: (Box<RawExpression>, Box<RawExpression>),
        },
        /// Not expression.
        /// Evaluates to the negation of its sub-expression.
        Not { not: Box<RawExpression> },

        /// Raw sub-expression. Will be parsed before being turned in a real expression.
        Raw { raw: String },

        /// Empty expression.
        /// Evaluates to all items resolvable by the resolver.
        Empty,
    }

    impl TryFrom<RawExpression> for Expression<ExpressionField> {
        type Error = anyhow::Error;

        fn try_from(value: RawExpression) -> Result<Self, Self::Error> {
            let expr = match value {
                RawExpression::Empty => Self::Empty,
                RawExpression::Raw { raw } => {
                    Expression::parse(raw).map_err(|e| anyhow!("{}", e))?
                }
                RawExpression::Not { not } => Self::Not {
                    not: Box::new(Expression::try_from(*not)?),
                },
                RawExpression::Or { or: (a, b) } => Self::Or {
                    or: (
                        Box::new(Expression::try_from(*a)?),
                        Box::new(Expression::try_from(*b)?),
                    ),
                },
                RawExpression::And { and: (a, b) } => Self::And {
                    and: (
                        Box::new(Expression::try_from(*a)?),
                        Box::new(Expression::try_from(*b)?),
                    ),
                },
                RawExpression::HasKey { key } => Self::Field(ExpressionField::HasKey { key }),
                RawExpression::Field { key, value } => {
                    Self::Field(ExpressionField::Field { key, value })
                }
                RawExpression::Tag { tag } => Self::Field(ExpressionField::Tag { tag }),
            };

            Ok(expr)
        }
    }

    #[derive(Clone, Debug, Deserialize, Hash, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    #[serde(untagged)]
    pub enum ExpressionRequest {
        Expr(RawExpression),
        Raw(String),
    }

    impl Default for ExpressionRequest {
        fn default() -> Self {
            ExpressionRequest::Expr(RawExpression::Empty)
        }
    }

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

    fn default_sort_order() -> SortOrder {
        SortOrder::CreationAscending
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

        #[serde(default = "default_sort_order")]
        pub sort_order: SortOrder,
    }

    impl TryFrom<QueryRequest> for Query {
        type Error = anyhow::Error;

        fn try_from(request: QueryRequest) -> Result<Self, Self::Error> {
            let expression = match request.expression {
                ExpressionRequest::Expr(e) => Expression::try_from(e)?,
                ExpressionRequest::Raw(raw) => Expression::parse(raw)?,
            };

            Ok(Query {
                expression,
                from: request.from,
                size: request.size,
                sign_urls: request.sign_urls,
                facets: request.facets,
                sort_order: request.sort_order,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use interface::{Expression, ExpressionField};

    use super::query::RawExpression;

    #[test]
    fn mixed_expression_parsing() {
        let manual_expr = Expression::And {
            and: (
                Box::new(Expression::Field(ExpressionField::Tag { tag: "a".into() })),
                Box::new(Expression::And {
                    and: (
                        Box::new(Expression::Field(ExpressionField::Tag { tag: "b".into() })),
                        Box::new(Expression::Field(ExpressionField::Tag { tag: "c".into() })),
                    ),
                }),
            ),
        };

        let auto_expr = Expression::try_from(RawExpression::And {
            and: (
                Box::new(RawExpression::Tag {
                    tag: String::from("a"),
                }),
                Box::new(RawExpression::Raw {
                    raw: String::from("b && c"),
                }),
            ),
        })
        .unwrap();

        assert_eq!(manual_expr, auto_expr);
    }
}
