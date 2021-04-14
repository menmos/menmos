use std::convert::TryFrom;

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
    use super::*;

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
}
