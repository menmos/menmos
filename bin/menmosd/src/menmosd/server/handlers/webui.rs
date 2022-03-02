use axum::body::Body;
use axum::extract::Path;
use axum::http;

#[cfg(feature = "webui")]
use include_dir::{include_dir, Dir};

use apikit::reject::HTTPError;

#[cfg(feature = "webui")]
const STATIC_FILE_DIR: Dir = include_dir!("$CARGO_MANIFEST_DIR/menmos-web/dist");

#[tracing::instrument]
#[cfg(feature = "webui")]
pub async fn serve_static(Path(path): Path<String>) -> Result<http::Response<Body>, HTTPError> {
    let path = path
        .strip_prefix('/')
        .map(|f| String::from(f))
        .unwrap_or_default();

    let path = if path.is_empty() {
        "index.html"
    } else {
        path.as_ref()
    };

    for file in STATIC_FILE_DIR.entries() {
        tracing::debug!("got entry: {:?}", file.path())
    }

    let memory_file = STATIC_FILE_DIR.get_file(path).unwrap_or_else(|| {
        tracing::debug!("falling back on index.html");
        STATIC_FILE_DIR.get_file("index.html").unwrap()
    });

    let mut resp = Response::builder();

    if let Some(mimetype) = menmos_std::fs::mimetype(memory_file.path()) {
        tracing::trace!("got mime: {mimetype}");
        resp = resp.header("content-type", mimetype);
    }

    resp.body(Body::from(memory_file.contents()))
        .map_err(HTTPError::internal_server_error)
}

#[cfg(not(feature = "webui"))]
pub async fn serve_static(Path(_): Path<String>) -> Result<http::Response<Body>, HTTPError> {
    tracing::warn!("menmos-web is not enabled");
    Err(HTTPError::NotFound)
}
