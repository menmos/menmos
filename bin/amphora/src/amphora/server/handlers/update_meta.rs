use std::sync::Arc;

use apikit::{auth::UserIdentity, reject::InternalServerError};

use interface::{BlobInfoRequest, BlobMetaRequest, StorageNode};

pub async fn update_meta<N: StorageNode>(
    user: UserIdentity,
    node: Arc<N>,
    blob_id: String,
    meta_request: BlobMetaRequest,
) -> Result<warp::reply::Response, warp::Rejection> {
    node.update_meta(
        blob_id,
        BlobInfoRequest {
            meta_request,
            owner: user.username,
        },
    )
    .await
    .map_err(InternalServerError::from)?;

    Ok(apikit::reply::message("OK"))
}
