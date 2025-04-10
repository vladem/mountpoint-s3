use crate::common::fuse::{
    self, read_dir_to_entry_names, TestClient, TestSession, TestSessionConfig, TestSessionCreator,
};
use mountpoint_s3_fs::manifest::builder::create_db_from_slice;
use mountpoint_s3_fs::manifest::ManifestEntry;
use mountpoint_s3_fs::S3FilesystemConfig;
use std::fs::{self, metadata};
use std::io::ErrorKind;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use test_case::test_case;

#[test_case(&[
    "dir1/a.txt",
    "dir1/dir2/b.txt",
    "dir1/dir2/c.txt",
    "dir1/dir3/dir4/d.txt",
    "e.txt",
], &[], "", &mut ["dir1", "e.txt"]; "root directory")]
#[test_case(&[
    "dir1/a.txt",
    "dir1/dir2/b.txt",
    "dir1/dir2/c.txt",
    "dir1/dir3/dir4/d.txt",
    "e.txt",
], &[], "dir1", &["a.txt", "dir2", "dir3"]; "child directory")]
#[test_case(&[
    "dir1/a.txt",
    "dir1/dir2/b.txt",
], &[
    "dir1/dir2/c.txt",
    "dir1/dir3/dir4/d.txt",
    "dir1/e.txt",
    "f.txt",
], "dir1", &["a.txt", "dir2"]; "with excluded keys")]
fn test_readdir_manifest(
    manifest_keys: &[&str],
    excluded_keys: &[&str],
    directory_to_list: &str,
    expected_children: &[&str],
) {
    let (_tmp_dir, db_path) = create_dummy_manifest(manifest_keys, 0);
    let test_session = create_test_session_with_manifest(fuse::mock_session::new, &db_path);
    put_dummy_objects(test_session.client(), manifest_keys, excluded_keys);

    let read_dir_iter = fs::read_dir(test_session.mount_path().join(directory_to_list)).unwrap();
    let dir_entry_names = read_dir_to_entry_names(read_dir_iter);
    assert_eq!(dir_entry_names, expected_children, "readdir test failed");
}

#[test]
fn test_readdir_manifest_20k_keys() {
    let manifest_keys = (0..20000).map(|i| format!("dir1/file_{}", i)).collect::<Vec<_>>();
    let excluded_keys = &["dir1/excluded_file".to_string()];
    let directory_to_list = "dir1";
    let mut expected_children = (0..20000).map(|i| format!("file_{}", i)).collect::<Vec<_>>();
    expected_children.sort(); // children are expected to be in the sorted order

    let (_tmp_dir, db_path) = create_dummy_manifest(&manifest_keys, 0);
    let test_session = create_test_session_with_manifest(fuse::mock_session::new, &db_path);
    put_dummy_objects(test_session.client(), &manifest_keys, excluded_keys);

    let read_dir_iter = fs::read_dir(test_session.mount_path().join(directory_to_list)).unwrap();
    let dir_entry_names = read_dir_to_entry_names(read_dir_iter);
    assert_eq!(dir_entry_names, expected_children, "readdir test failed");
}

#[test]
fn test_lookup_unicode_keys_manifest() {
    // todo: are non UTF-8 keys supported?
    let file_size = 1024;
    let keys = &["مرحبًا", "🇦🇺", "🐈/🦀", "a\0"];
    let excluded_keys = &["こんにちは"];
    let (_tmp_dir, db_path) = create_dummy_manifest(keys, file_size);
    let test_session = create_test_session_with_manifest(fuse::mock_session::new, &db_path);
    put_dummy_objects(test_session.client(), keys, excluded_keys);

    let m = metadata(test_session.mount_path().join("مرحبًا")).unwrap();
    assert!(m.file_type().is_file());
    assert_eq!(m.size(), file_size as u64);
    let m = metadata(test_session.mount_path().join("🇦🇺")).unwrap();
    assert!(m.file_type().is_file());
    let m = metadata(test_session.mount_path().join("🐈")).unwrap();
    assert!(m.file_type().is_dir());
    let m = metadata(test_session.mount_path().join("🐈/🦀")).unwrap();
    assert!(m.file_type().is_file());
    let e = metadata(test_session.mount_path().join("a\0")).expect_err("must not exist");
    assert_eq!(e.kind(), ErrorKind::InvalidInput); // fs API does not allow using \0 in file names
    let e = metadata(test_session.mount_path().join("こんにちは")).expect_err("must not exist");
    assert_eq!(e.kind(), ErrorKind::NotFound);
}

// fn test_basic_read_manifest_s3()
// fn test_manifest_forbidden_operations()
// fn test_read_manifest_wrong_etag()
// fn test_read_manifest_wrong_size()
// fn test_read_manifest_missing_etag()
// fn test_read_manifest_missing_size()

fn create_test_session_with_manifest(creator_fn: impl TestSessionCreator, db_path: &Path) -> TestSession {
    let prefix = "";

    let filesystem_config = S3FilesystemConfig {
        manifest_db_path: Some(db_path.to_path_buf()),
        ..Default::default()
    };
    creator_fn(
        prefix,
        TestSessionConfig {
            filesystem_config,
            ..Default::default()
        },
    )
}

fn create_dummy_manifest<T: AsRef<str>>(s3_keys: &[T], file_size: usize) -> (TempDir, PathBuf) {
    let db_dir = tempfile::tempdir().unwrap();
    let db_path = db_dir.path().join("s3_keys.db3");

    let db_entries: Vec<_> = s3_keys
        .iter()
        .map(|key| ManifestEntry::File {
            full_key: key.as_ref().to_string(),
            etag: "".to_owned(),
            size: file_size,
        })
        .collect();
    create_db_from_slice(&db_path, &db_entries).expect("db must be created");

    (db_dir, db_path)
}

fn put_dummy_objects<T: AsRef<str>>(test_client: &dyn TestClient, manifest_keys: &[T], excluded_keys: &[T]) {
    for name in manifest_keys.iter().chain(excluded_keys.iter()) {
        let content = vec![b'0'; 1024];
        test_client.put_object(name.as_ref(), &content).unwrap();
    }
}
