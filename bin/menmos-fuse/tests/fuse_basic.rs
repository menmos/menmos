use std::path::PathBuf;

use anyhow::Result;

use menmos_client::Meta;

use menmos_fs::config::{ClientConfig, Contents, Mount};
use menmos_fs::MenmosFS;

use testing::fixtures::Menmos;

#[tokio::test]
async fn basic_lookup_and_read() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    cluster
        .push_document("bing bong", Meta::file("yayeet.txt").with_tag("testfile"))
        .await?;

    let fs = MenmosFS::new(Mount {
        name: String::from("test_mount"),
        client: ClientConfig::Host {
            host: cluster.directory_url,
            password: cluster.directory_password,
        },
        mount_point: PathBuf::from("/tmp"), // We won't mount it so we don't really care about the path.
        contents: Contents::Query {
            expression: String::from("testfile"),
            group_by_tags: false,
            group_by_meta_keys: Vec::default(),
        },
    })
    .await?;

    let r = fs.lookup_impl(1, "yayeet.txt".as_ref()).await?;
    let read_data = fs.read_impl(r.attrs.ino, 0, 4096).await?;
    assert_eq!(&read_data.data, b"bing bong");

    Ok(())
}
