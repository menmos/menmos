use std::net::IpAddr;

use anyhow::Result;

use chrono::Utc;

use interface::{
    BlobInfo, BlobMetaRequest, DirectoryNode, Query, QueryResponse, RoutingConfig, StorageNodeInfo,
    Type,
};

use crate::Directory;

use super::mock;

fn get_storage_node_info(name: &str) -> StorageNodeInfo {
    StorageNodeInfo {
        id: String::from(name),
        redirect_info: interface::RedirectInfo::Static {
            static_address: IpAddr::from([192, 168, 0, 1]),
        },
        port: 3031,
    }
}

async fn index<S: AsRef<str>>(
    id: S,
    meta_request: BlobMetaRequest,
    node: &Directory,
) -> StorageNodeInfo {
    let tgt_storage_node = node
        .indexer()
        .pick_node_for_blob(id.as_ref(), meta_request.clone(), "admin")
        .await
        .unwrap();

    node.indexer()
        .index_blob(
            id.as_ref(),
            BlobInfo {
                owner: "admin".to_string(),
                meta: meta_request.into_meta(Utc::now(), Utc::now()),
            },
            &tgt_storage_node.id,
        )
        .await
        .unwrap();
    tgt_storage_node
}

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
async fn register_storage_node_ok() {
    let node = mock::node();
    assert!(node
        .admin()
        .register_storage_node(get_storage_node_info("alpha"))
        .await
        .is_ok())
}

#[tokio::test]
async fn pick_node_for_blob_with_single_node() {
    let node = mock::node();

    let storage = get_storage_node_info("alpha");
    node.admin()
        .register_storage_node(storage.clone())
        .await
        .unwrap();

    let actual = index("bing", BlobMetaRequest::new("somename", Type::File), &node).await;

    assert_eq!(storage, actual);
}

#[tokio::test]
async fn add_multiblob_round_robin() {
    let node = mock::node();

    let storage_nodes = vec![
        get_storage_node_info("alpha"),
        get_storage_node_info("beta"),
        get_storage_node_info("gamma"),
    ];

    for n in storage_nodes.clone().into_iter() {
        node.admin().register_storage_node(n).await.unwrap();
    }

    for i in 0..100_i32 {
        let tgt_storage_node = index(
            &format!("{}", i),
            BlobMetaRequest::new("somename", Type::File),
            &node,
        )
        .await;
        let expected_node = storage_nodes.get((i % 3) as usize).unwrap();
        assert_eq!(&tgt_storage_node, expected_node);
    }
}

#[tokio::test]
async fn get_blob_node_multiblob() {
    let node = mock::node();

    let storage_nodes = vec![
        get_storage_node_info("alpha"),
        get_storage_node_info("beta"),
        get_storage_node_info("gamma"),
    ];

    for n in storage_nodes.clone().into_iter() {
        node.admin().register_storage_node(n).await.unwrap();
    }

    for i in 0..100_i32 {
        let blob_id = format!("{}", i);
        index(
            &blob_id,
            BlobMetaRequest::new("somename", Type::File),
            &node,
        )
        .await;

        let tgt_storage_node = node
            .indexer()
            .get_blob_storage_node(&blob_id)
            .await
            .unwrap()
            .unwrap();

        let expected_node = storage_nodes.get((i % 3) as usize).unwrap();
        assert_eq!(&tgt_storage_node, expected_node);
    }
}

#[tokio::test]
async fn get_nonexistent_blob() {
    let node = mock::node();
    assert_eq!(
        node.indexer().get_blob_storage_node("asdf").await.unwrap(),
        None
    );
}

#[tokio::test]
async fn empty_query_empty_node() {
    let node = mock::node();

    let r = node
        .query()
        .query(&Query::default(), "admin")
        .await
        .unwrap();
    assert_eq!(
        r,
        QueryResponse {
            count: 0,
            total: 0,
            hits: Vec::default(),
            facets: None
        }
    );
}

#[tokio::test]
async fn query_single_tag() {
    let node = mock::node();
    node.admin()
        .register_storage_node(get_storage_node_info("alpha"))
        .await
        .unwrap();

    index(
        "alpha",
        BlobMetaRequest::new("somename", Type::File).with_tag("world"),
        &node,
    )
    .await;
    index(
        "beta",
        BlobMetaRequest::new("somename", Type::File).with_tag("hello"),
        &node,
    )
    .await;

    let r = node
        .query()
        .query(&Query::default().and_tag("hello"), "admin")
        .await
        .unwrap();

    assert_eq!(r.total, 1);
    assert_eq!(r.count, 1);
    assert_eq!(r.hits[0].id, "beta");
}

#[tokio::test]
async fn query_single_kv() {
    let node = mock::node();
    node.admin()
        .register_storage_node(get_storage_node_info("alpha"))
        .await
        .unwrap();

    index(
        "alpha",
        BlobMetaRequest::new("somename", Type::File).with_meta("hello", "there"),
        &node,
    )
    .await;

    index(
        "beta",
        BlobMetaRequest::new("somename", Type::File).with_meta("hello", "world"),
        &node,
    )
    .await;

    let r = node
        .query()
        .query(&Query::default().and_meta("hello", "world"), "admin")
        .await
        .unwrap();

    assert_eq!(r.total, 1);
    assert_eq!(r.count, 1);
    assert_eq!(r.hits[0].id, "beta");
}

#[tokio::test]
async fn query_multi_tag() {
    let node = mock::node();
    node.admin()
        .register_storage_node(get_storage_node_info("alpha"))
        .await
        .unwrap();

    index(
        "alpha",
        BlobMetaRequest::new("somename", Type::File).with_tag("world"),
        &node,
    )
    .await;

    index(
        "beta",
        BlobMetaRequest::new("somename", Type::File)
            .with_tag("hello")
            .with_tag("world"),
        &node,
    )
    .await;

    index(
        "gamma",
        BlobMetaRequest::new("somename", Type::File).with_tag("there"),
        &node,
    )
    .await;

    let r = node
        .query()
        .query(&Query::default().and_tag("hello").and_tag("world"), "admin")
        .await
        .unwrap();

    assert_eq!(r.total, 1);
    assert_eq!(r.count, 1);
    assert_eq!(r.hits[0].id, "beta");
}

#[tokio::test]
async fn query_single_tag_no_match() {
    let node = mock::node();
    node.admin()
        .register_storage_node(get_storage_node_info("alpha"))
        .await
        .unwrap();

    index(
        "alpha",
        BlobMetaRequest::new("somename", Type::File).with_tag("world"),
        &node,
    )
    .await;
    index(
        "beta",
        BlobMetaRequest::new("somename", Type::File).with_tag("hello"),
        &node,
    )
    .await;

    let r = node
        .query()
        .query(&Query::default().and_tag("bing"), "admin")
        .await
        .unwrap();

    assert_eq!(
        r,
        QueryResponse {
            count: 0,
            total: 0,
            hits: Vec::default(),
            facets: None
        }
    );
}

#[tokio::test]
async fn query_children() -> Result<()> {
    let node = mock::node();
    node.admin()
        .register_storage_node(get_storage_node_info("alpha"))
        .await?;

    index(
        "mydirectory",
        BlobMetaRequest::new("somename", Type::File),
        &node,
    )
    .await;
    index(
        "beta",
        BlobMetaRequest::new("somename", Type::File).with_parent("mydirectory"),
        &node,
    )
    .await;
    index(
        "gamma",
        BlobMetaRequest::new("somename", Type::File).with_parent("mydirectory"),
        &node,
    )
    .await;
    index(
        "omega",
        BlobMetaRequest::new("somename", Type::File).with_parent("otherdirectory"),
        &node,
    )
    .await;

    let r = node
        .query()
        .query(&Query::default().and_parent("mydirectory"), "admin")
        .await
        .unwrap();

    assert_eq!(r.total, 2);
    assert_eq!(r.count, 2);
    assert_eq!(
        r.hits.iter().map(|h| h.id.clone()).collect::<Vec<_>>(),
        vec!["beta".to_string(), "gamma".to_string()]
    );

    Ok(())
}

#[tokio::test]
async fn list_metadata_tags() -> Result<()> {
    let node = mock::node();
    node.admin()
        .register_storage_node(get_storage_node_info("alpha"))
        .await?;

    index(
        "alpha",
        BlobMetaRequest::new("somename", Type::File).with_tag("bing"),
        &node,
    )
    .await;
    index(
        "beta",
        BlobMetaRequest::new("somename", Type::File).with_tag("bing"),
        &node,
    )
    .await;
    index(
        "gamma",
        BlobMetaRequest::new("somename", Type::File).with_tag("bong"),
        &node,
    )
    .await;

    let r = node
        .query()
        .list_metadata(Some(vec!["bing".to_string()]), None, "admin")
        .await?;

    assert_eq!(r.tags.len(), 1);
    assert_eq!(r.tags["bing"], 2);
    assert_eq!(r.meta.len(), 0);

    Ok(())
}

#[tokio::test]
async fn document_deletion_missing_document_with_not() -> Result<()> {
    let node = mock::node();
    node.admin()
        .register_storage_node(get_storage_node_info("alpha"))
        .await?;

    index("alpha", BlobMetaRequest::new("somename", Type::File), &node).await;
    index("beta", BlobMetaRequest::new("somename", Type::File), &node).await;

    let results = node.query().query(&Query::default(), "admin").await?;
    assert_eq!(results.total, 2);

    node.indexer().delete_blob("alpha", "alpha").await?;

    let results = node
        .query()
        .query(&Query::default().with_expression("!bing")?, "admin")
        .await?;
    assert_eq!(results.total, 1);

    Ok(())
}

#[tokio::test]
async fn faceting_basic() -> Result<()> {
    let node = mock::node();
    node.admin()
        .register_storage_node(get_storage_node_info("alpha"))
        .await?;

    index(
        "alpha",
        BlobMetaRequest::new("somename", Type::File)
            .with_tag("a")
            .with_meta("hello", "world"),
        &node,
    )
    .await;

    index(
        "beta",
        BlobMetaRequest::new("somename", Type::File)
            .with_tag("b")
            .with_meta("hello", "world"),
        &node,
    )
    .await;

    index(
        "gamma",
        BlobMetaRequest::new("somename", Type::File)
            .with_tag("a")
            .with_meta("hello", "there"),
        &node,
    )
    .await;

    let res = node
        .query()
        .query(&Query::default().with_facets(true), "admin")
        .await?;

    let facets = res.facets.unwrap();
    assert_eq!(facets.tags.len(), 2);
    assert_eq!(facets.meta.len(), 1);
    assert_eq!(facets.meta["hello"].len(), 2);
    assert_eq!(facets.tags["a"], 2);
    assert_eq!(facets.tags["b"], 1);
    assert_eq!(facets.meta["hello"]["world"], 2);
    assert_eq!(facets.meta["hello"]["there"], 1);

    Ok(())
}

#[tokio::test]
async fn facet_grouping() -> Result<()> {
    let node = mock::node();
    node.admin()
        .register_storage_node(get_storage_node_info("alpha"))
        .await?;

    index(
        "alpha",
        BlobMetaRequest::new("somename", Type::File)
            .with_tag("a")
            .with_meta("hello", "world"),
        &node,
    )
    .await;

    index(
        "beta",
        BlobMetaRequest::new("somename", Type::File)
            .with_tag("b")
            .with_meta("hello", "world"),
        &node,
    )
    .await;

    index(
        "gamma",
        BlobMetaRequest::new("somename", Type::File)
            .with_tag("a")
            .with_meta("hello", "there"),
        &node,
    )
    .await;

    let res = node
        .query()
        .query(&Query::default().and_tag("a").with_facets(true), "admin")
        .await?;

    let facets = res.facets.unwrap();

    assert_eq!(facets.tags.len(), 1);
    assert_eq!(facets.meta.len(), 1);
    assert_eq!(facets.meta["hello"].len(), 2);
    assert_eq!(facets.tags["a"], 2);
    assert_eq!(facets.meta["hello"]["world"], 1);
    assert_eq!(facets.meta["hello"]["there"], 1);

    Ok(())
}

#[tokio::test]
async fn routing_info_get_set_delete() -> Result<()> {
    let node = mock::node();

    let cfg = RoutingConfig::new("some_field").with_route("alpha", "beta");

    assert_eq!(node.routing().get_routing_config("jdoe").await?, None);

    node.routing().set_routing_config("jdoe", &cfg).await?;

    assert_eq!(
        &node.routing().get_routing_config("jdoe").await?.unwrap(),
        &cfg
    );

    node.routing().delete_routing_config("jdoe").await?;

    assert_eq!(node.routing().get_routing_config("jdoe").await?, None);

    Ok(())
}

#[tokio::test]
async fn add_multi_blob_routing_key() -> Result<()> {
    let node = mock::node();

    let mut storage_nodes = Vec::with_capacity(3);
    storage_nodes.push(get_storage_node_info("alpha"));
    storage_nodes.push(get_storage_node_info("beta"));

    for storage_node in storage_nodes.into_iter() {
        node.admin().register_storage_node(storage_node).await?;
    }

    let cfg = RoutingConfig::new("some_field")
        .with_route("a", "alpha")
        .with_route("b", "beta");

    node.routing().set_routing_config("admin", &cfg).await?;

    // Test each multiple times so we know it's not round-robin.
    for _ in 0..10 {
        let node = node
            .indexer()
            .pick_node_for_blob(
                "asdf",
                BlobMetaRequest::file("bing.txt").with_meta("some_field", "a"),
                "admin",
            )
            .await?;
        assert_eq!(node.id, "alpha");
    }

    for _ in 0..10 {
        let node = node
            .indexer()
            .pick_node_for_blob(
                "asdf",
                BlobMetaRequest::file("bing.txt").with_meta("some_field", "b"),
                "admin",
            )
            .await?;
        assert_eq!(node.id, "beta");
    }

    Ok(())
}

#[tokio::test]
async fn add_blob_routing_key_unknown_value() -> Result<()> {
    let node = mock::node();

    let mut storage_nodes = Vec::with_capacity(3);
    storage_nodes.push(get_storage_node_info("alpha"));
    storage_nodes.push(get_storage_node_info("beta"));

    for storage_node in storage_nodes.into_iter() {
        node.admin().register_storage_node(storage_node).await?;
    }

    let cfg = RoutingConfig::new("some_field")
        .with_route("a", "alpha")
        .with_route("b", "beta");

    node.routing().set_routing_config("admin", &cfg).await?;

    for i in 0..10 {
        let node = node
            .indexer()
            .pick_node_for_blob(
                "asdf",
                BlobMetaRequest::file("bing.txt").with_meta("some_field", "unknown"),
                "admin",
            )
            .await?;

        let expected_node = if i % 2 == 0 { "alpha" } else { "beta" };
        assert_eq!(node.id, expected_node);
    }

    Ok(())
}

#[tokio::test]
async fn add_blob_routing_key_missing_storage_node() -> Result<()> {
    let node = mock::node();

    // We only register node alpha.
    node.admin()
        .register_storage_node(get_storage_node_info("alpha"))
        .await?;

    // We put alpha *and* beta in the routing config.
    let cfg = RoutingConfig::new("some_field")
        .with_route("a", "alpha")
        .with_route("b", "beta");

    node.routing().set_routing_config("admin", &cfg).await?;

    assert!(node
        .indexer()
        .pick_node_for_blob(
            "asdf",
            BlobMetaRequest::file("bing.txt").with_meta("some_field", "b"),
            "admin"
        )
        .await
        .is_err());

    Ok(())
}
