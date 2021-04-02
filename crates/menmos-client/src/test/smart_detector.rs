use std::path::Path;

use crate::smart_detector::SmartDetector;

#[test]
fn detect_file_mime_type() {
    let path = Path::new("foo.txt");
    let smart_detector = SmartDetector::new().unwrap();

    let mime_type = smart_detector.detect(path);

    assert!(mime_type.is_some());
    assert_eq!(mime_type.unwrap(), "text/plain");
}

#[test]
fn detect_no_mime_type() {
    let path = Path::new("foo.invalid");
    let smart_detector = SmartDetector::new().unwrap();

    let mime_type = smart_detector.detect(path);

    assert!(mime_type.is_none());
}
