use headers::HeaderValue;
use include_dir::{include_dir, Dir};

use warp::hyper::Body;

#[cfg(feature = "webui")]
const STATIC_FILE_DIR: Dir = include_dir!("./menmos-web/out");

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

#[tracing::instrument]
pub async fn serve_static(path: warp::path::Tail) -> Result<impl warp::Reply, warp::Rejection> {
    let file_name = if path.as_str().is_empty() {
        "index.html"
    } else {
        path.as_str()
    };

    if cfg!(feature = "webui") {
        match STATIC_FILE_DIR.get_file(file_name) {
            Some(file_path) => {
                let body = Body::from(file_path.contents().to_vec());
                Ok(warp::reply::Response::new(body))
            }
            None => Ok(serve(
                "<html><body><h1>Not Found</h1></body></html>",
                "application/html",
            )),
        }
    } else {
        tracing::warn!("menmos-web is not enabled");
        Ok(serve(
            "<html><body><h1>Not Found</h1></body></html>",
            "application/html",
        ))
    }
}
