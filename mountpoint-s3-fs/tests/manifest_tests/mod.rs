use crate::common::manifest::{create_dummy_manifest, DUMMY_SIZE};
use mountpoint_s3_fs::manifest::ManifestError;
use test_case::test_case;

#[test_case("dir1/./a.txt"; "with dot")]
#[test_case("dir1/../a.txt"; "with 2 dots")]
#[test_case("dir1//a.txt"; "with 2 slashes")]
#[test_case(""; "empty")]
#[test_case("dir1/a\0.txt"; "with 0")]
#[test_case("dir1/dir2/"; "ends with slash")]
fn test_ingest_invalid_key(key: &str) {
    let err = create_dummy_manifest(&[key], DUMMY_SIZE).expect_err("must be an error");
    assert!(matches!(err, ManifestError::InvalidKey(_)));
}
