use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;

use interface::{
    BlobIndexer, DirectoryNode, NodeAdminController, QueryExecutor, RoutingConfigManager,
    StorageNodeInfo, UserManagement,
};

pub struct Directory {
    indexer: Arc<Box<dyn BlobIndexer + Send + Sync>>,
    router: Arc<Box<dyn RoutingConfigManager + Send + Sync>>,
    admin: Arc<Box<dyn NodeAdminController + Send + Sync>>,
    user: Arc<Box<dyn UserManagement + Send + Sync>>,
    query: Arc<Box<dyn QueryExecutor + Send + Sync>>,

    rebuild_queue: Mutex<Vec<StorageNodeInfo>>,
}

impl Directory {
    pub fn new(
        indexer: Arc<Box<dyn BlobIndexer + Send + Sync>>,
        router: Arc<Box<dyn RoutingConfigManager + Send + Sync>>,
        admin: Arc<Box<dyn NodeAdminController + Send + Sync>>,
        user: Arc<Box<dyn UserManagement + Send + Sync>>,
        query: Arc<Box<dyn QueryExecutor + Send + Sync>>,
    ) -> Self {
        Self {
            indexer,
            router,
            admin,
            user,
            query,
            rebuild_queue: Default::default(),
        }
    }
}

#[async_trait]
impl DirectoryNode for Directory {
    fn indexer(&self) -> Arc<Box<dyn BlobIndexer + Send + Sync>> {
        self.indexer.clone()
    }
    fn routing(&self) -> Arc<Box<dyn RoutingConfigManager + Send + Sync>> {
        self.router.clone()
    }
    fn admin(&self) -> Arc<Box<dyn NodeAdminController + Send + Sync>> {
        self.admin.clone()
    }
    fn user(&self) -> Arc<Box<dyn UserManagement + Send + Sync>> {
        self.user.clone()
    }
    fn query(&self) -> Arc<Box<dyn QueryExecutor + Send + Sync>> {
        self.query.clone()
    }

    async fn commit(&self) -> Result<()> {
        let (a, b, c, d) = tokio::join!(
            self.indexer.commit(),
            self.router.commit(),
            self.admin.commit(),
            self.user.commit(),
        );

        a?;
        b?;
        c?;
        d?;

        Ok(())
    }
}
