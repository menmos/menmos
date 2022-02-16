use std::collections::HashMap;
use std::path::Path;

use once_cell::sync::OnceCell;

const MIMETYPES_BYTES: &[u8] = include_bytes!("data/mime-types.json");

fn mimetype_map() -> &'static HashMap<String, String> {
    static MIME_MAP: OnceCell<HashMap<String, String>> = OnceCell::new();
    MIME_MAP.get_or_init(|| {
        match serde_json::from_slice::<HashMap<String, String>>(&MIMETYPES_BYTES) {
            Ok(map) => map,
            Err(e) => {
                // We're ok with panicking here, this can only happen if we ship invalid JSON.
                // If that happens, the tests will catch it.
                panic!("invalid meta map: {}", e);
            }
        }
    })
}

/// Try to get the mimetype of a given path.
///
/// The path doesn't have to exist.
///
/// # Examples
/// ```
/// let my_file = "test.txt";
///
/// let mimetype = menmos_std::fs::mimetype(my_file);
/// assert_eq!(mimetype, Some(String::from("text/plain")));
/// ```
pub fn mimetype<P: AsRef<Path>>(path: P) -> Option<String> {
    let data = mimetype_map();
    let extension = path.as_ref().extension()?.to_str()?;
    data.get(extension).cloned()
}

#[cfg(test)]
mod tests {
    use super::mimetype;

    #[test]
    fn detect_file_mime_type() {
        let path = "foo.html";
        let mime_type = mimetype(path);

        assert!(mime_type.is_some());
        assert_eq!(mime_type.unwrap(), "text/html");
    }

    #[test]
    fn detect_no_mime_type() {
        let path = "foo.invalid";
        let mime_type = mimetype(path);

        assert!(mime_type.is_none());
    }
}
