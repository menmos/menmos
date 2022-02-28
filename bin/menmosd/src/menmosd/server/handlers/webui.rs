use axum::body::{self, Body};
use axum::extract::Path;
use axum::http;
use axum::http::Request;
use axum::response::Response;
use bytes::Bytes;
use headers::HeaderValue;
use hyper::body::HttpBody;

#[cfg(feature = "webui")]
use include_dir::{include_dir, Dir};

use apikit::reject::HTTPError;

#[cfg(feature = "webui")]
const STATIC_FILE_DIR: Dir = include_dir!("$CARGO_MANIFEST_DIR/menmos-web/dist");

/*
fn serve(text: &str, content_type: &'static str) -> warp::http::Response<Body> {
    let body = Body::from(text.as_bytes().to_vec());
    let mut http_resp = warp::reply::Response::new(body);

    http_resp
        .headers_mut()
        .insert("content-type", HeaderValue::from_static(content_type));

    http_resp
        .headers_mut()
        .insert("access-control-allow-origin", HeaderValue::from_static("*"));

    http_resp
}
*/

#[tracing::instrument]
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

/*
#[cfg(feature = "webui")]
pub fn get_response(file_name: &str) -> warp::http::Response<Body> {
    match STATIC_FILE_DIR.get_file(file_name) {
        Some(file_path) => {
            let body = Body::from(file_path.contents().to_vec());
            let mut resp = warp::reply::Response::new(body);
            if let Some(mimetype) = menmos_std::fs::mimetype(file_path.path()) {
                if let Some(mimetype_header) = HeaderValue::from_str(&mimetype).ok() {
                    resp.headers_mut().insert("content-type", mimetype_header);
                }
            }
            resp
        }
        None => serve(
            "<html><body><h1>Not Found</h1></body></html>",
            "application/html",
        ),
    }
}

#[cfg(not(feature = "webui"))]
pub fn get_response(_file_name: &str) -> warp::http::Response<Body> {
    tracing::warn!("menmos-web is not enabled");
    serve(
        "<html><body><h1>Not Found</h1></body></html>",
        "application/html",
    )
}
 */
