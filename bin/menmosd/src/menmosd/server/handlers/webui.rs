use headers::HeaderValue;

#[cfg(feature = "webui")]
use include_dir::{include_dir, Dir};

use warp::hyper::Body;

#[cfg(feature = "webui")]
const STATIC_FILE_DIR: Dir = include_dir!("$CARGO_MANIFEST_DIR/menmos-web/dist");

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

#[tracing::instrument]
pub async fn serve_static(path: warp::path::Tail) -> Result<impl warp::Reply, warp::Rejection> {
    let file_name = if path.as_str().is_empty() {
        "index.html"
    } else {
        path.as_str()
    };

    Ok(get_response(file_name))
}
