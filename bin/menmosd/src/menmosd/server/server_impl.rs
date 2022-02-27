use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;

use axum::handler::Handler;
use axum::http::Request;
use axum::routing::*;
use axum::{AddExtensionLayer, Router};

use interface::{CertificateInfo, DirectoryNode, DynDirectoryNode};

use tokio::sync::mpsc;
use tokio::task::{spawn, JoinHandle};

use tower_http::trace::TraceLayer;
use tower_request_id::{RequestId, RequestIdLayer};

use crate::config::{Config, ServerSetting};
use crate::server::handlers;

use super::{context::Context, filters, ssl::use_tls};

pub(crate) fn build_router(
    config: Arc<Config>,
    node: DynDirectoryNode,
    certificate_info: Arc<Option<CertificateInfo>>,
) -> Router {
    Router::new()
        // Admin Routes
        .route("/health", get(handlers::admin::health))
        .route("/version", get(handlers::admin::version))
        .route("/rebuild", post(handlers::admin::rebuild))
        .route(
            "/rebuild/:storage_node_id",
            delete(handlers::admin::rebuild_complete),
        )
        .route("/flush", post(handlers::admin::flush))
        .route("/config", get(handlers::admin::get_config))
        // Auth routes
        .route("/auth/login", post(handlers::auth::login))
        .route("/auth/register", post(handlers::auth::register))
        // Query routes
        .route("/query", post(handlers::query::query))
        // Storage node routes
        .route(
            "/node/storage",
            put(handlers::storage::put).get(handlers::storage::list),
        )
        // Routing config routes
        .route(
            "/routing",
            get(handlers::routing::get)
                .put(handlers::routing::set)
                .delete(handlers::routing::delete),
        )
        // Blob meta routes
        .route("/metadata", get(handlers::blobmeta::list))
        .route(
            "/blob/:blob_id/metadata",
            get(handlers::blobmeta::get)
                .post(handlers::blobmeta::create)
                .put(handlers::blobmeta::update)
                .delete(handlers::blobmeta::delete),
        )
        // Blob routes
        .route("/blob", post(handlers::blob::put))
        .route(
            "/blob/:blob_id",
            get(handlers::blob::get)
                .post(handlers::blob::update)
                .put(handlers::blob::write)
                .delete(handlers::blob::delete),
        )
        .route("/blob/:blob_id/fsync", post(handlers::blob::fsync))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|r: &Request<_>| {
                    // We get the request id from the extensions
                    let request_id = r
                        .extensions()
                        .get::<RequestId>()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| "unknown".into());
                    // And then we put it along with other information into the `request` span
                    tracing::info_span!(
                        "request",
                        id = %request_id,
                        method = %r.method(),
                        uri = %r.uri(),
                    )
                })
                .on_request(|_r: &Request<_>, _s: &tracing::Span| {}), // We silence the on-request hook
        ) // TODO: Add on-response callback to log calls at the info level
        .layer(RequestIdLayer)
        .layer(AddExtensionLayer::new(certificate_info)) // TODO: Make this typing better
        .layer(AddExtensionLayer::new(config.node.encryption_key.clone())) // TODO: Make this typing better.
        .layer(AddExtensionLayer::new(config.clone()))
        .layer(AddExtensionLayer::new(node.clone()))
}

pub struct Server {
    node: Arc<dyn DirectoryNode + Send + Sync>,
    handle: JoinHandle<()>,
    stop_tx: mpsc::Sender<()>,
}

impl Server {
    pub async fn new<N: DirectoryNode + Send + Sync + 'static>(
        cfg: Config,
        node: N,
    ) -> Result<Server> {
        let node: Arc<dyn DirectoryNode + Send + Sync> = Arc::new(node);

        // Create the admin user.
        node.user()
            .register("admin", &cfg.node.admin_password)
            .await?;

        let config = Arc::new(cfg.clone());

        let (stop_tx, mut stop_rx) = mpsc::channel(1);

        let join_handle = match cfg.server {
            ServerSetting::Http(http_cfg) => {
                tracing::debug!("starting http layer");

                let srv = axum::Server::bind(&([0, 0, 0, 0], http_cfg.port).into())
                    .serve(
                        build_router(config, node.clone(), Arc::new(None))
                            .into_make_service_with_connect_info::<SocketAddr, _>(),
                    )
                    .with_graceful_shutdown(async move {
                        stop_rx.recv().await;
                    });

                tracing::debug!("http layer started");
                tracing::info!("menmosd is up");
                spawn(async move {
                    match srv.await {
                        Ok(_) => {}
                        Err(e) => {
                            tracing::error!("http server error: {e}");
                        }
                    }
                })
            }
            ServerSetting::Https(https_cfg) => {
                let node = node.clone();
                spawn(async move {
                    match use_tls(node, config, https_cfg, stop_rx).await {
                        Ok(_) => {}
                        Err(e) => {
                            tracing::error!("https server error: {e}")
                        }
                    }
                })
            }
        };

        Ok(Server {
            node,
            handle: join_handle,
            stop_tx,
        })
    }

    pub async fn stop(self) -> Result<()> {
        tracing::info!("requesting to quit");
        self.stop_tx.send(()).await?;
        self.handle.await?;
        self.node.flush().await?;
        tracing::info!("exited");

        Ok(())
    }
}
