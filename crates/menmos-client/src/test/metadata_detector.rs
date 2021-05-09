use crate::metadata_detector::MetadataDetector;

use crate::{Meta, Type};

#[test]
fn detect_file_mime_type() {
    let path = "foo.txt";
    let mut meta = Meta::new("test", Type::File);
    let meta_detector = MetadataDetector::new().unwrap();

    assert_eq!(meta.metadata.keys().count(), 0);

    meta_detector.populate(path, &mut meta).unwrap();

    assert_eq!(meta.metadata.keys().count(), 1);
}

#[test]
fn detect_no_mime_type() {
    let path = "foo.invalid";
    let mut meta = Meta::new("test", Type::File);
    let meta_detector = MetadataDetector::new().unwrap();

    assert_eq!(meta.metadata.keys().count(), 0);

    meta_detector.populate(path, &mut meta).unwrap();

    assert_eq!(meta.metadata.keys().count(), 0);
}
