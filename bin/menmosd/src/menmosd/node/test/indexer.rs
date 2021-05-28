use interface::{BlobIndexer, BlobMetaRequest, DirectoryNode, Type};

use super::mock;

#[tokio::test]
async fn pick_node_for_blob_with_no_storage_nodes() {
    let node = mock::node();
    assert!(node
        .indexer()
        .pick_node_for_blob(
            "bing",
            BlobMetaRequest::new("somename", Type::File),
            "admin"
        )
        .await
        .is_err());
}

#[tokio::test]
async fn pick_node_for_blob_with_single_node() {
    let node = mock::node();

    let storage = get_storage_node_info("alpha");
    node.register_storage_node(storage.clone()).await.unwrap();

    let actual = index("bing", BlobMetaRequest::new("somename", Type::File), &node).await;

    assert_eq!(storage, actual);
}
