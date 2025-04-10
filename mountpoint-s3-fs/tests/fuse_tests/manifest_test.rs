use crate::common::fuse::{self, read_dir_to_entry_names, TestClient, TestSessionConfig};
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
    let test_session = fuse::mock_session::new("", manifest_test_session_config(&db_path));
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
    let test_session = fuse::mock_session::new("", manifest_test_session_config(&db_path));
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
    let test_session = fuse::mock_session::new("", manifest_test_session_config(&db_path));
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

#[cfg(feature = "s3_tests")]
#[test_case(false, false; "just read")]
#[test_case(true, false; "readdir then read")]
#[test_case(false, true; "stat then read")]
#[tokio::test]
async fn test_basic_read_manifest_s3(readdir_before_read: bool, stat_before_read: bool) {
    use crate::common::s3::{get_test_bucket_and_prefix, get_test_region, get_test_sdk_client};
    use std::{fs::File, io::Read};

    let visible_object = ("visible_object_key", vec![b'1'; 1024]);
    let invisible_object = ("invisible_object_key", vec![b'2'; 1024]);

    // put objects
    let (bucket, prefix) = get_test_bucket_and_prefix("test_basic_read_manifest_s3");
    let sdk_client = get_test_sdk_client(&get_test_region()).await;
    let visible_object_props = put_object(
        &sdk_client,
        &bucket,
        &prefix,
        visible_object.0,
        visible_object.1.clone(),
    )
    .await;
    put_object(&sdk_client, &bucket, &prefix, invisible_object.0, invisible_object.1).await;

    // create manifest and do the mount
    let (_tmp_dir, db_path) = create_manifest(&[ManifestEntry::File {
        full_key: format!("{}{}", prefix, visible_object.0),
        etag: visible_object_props.0,
        size: visible_object_props.1,
    }]);
    let test_session =
        fuse::s3_session::new_with_test_client(manifest_test_session_config(&db_path), sdk_client, &bucket, &prefix);

    // if configured so, readdir before read
    if readdir_before_read {
        let read_dir_iter = fs::read_dir(test_session.mount_path()).unwrap();
        let dir_entry_names = read_dir_to_entry_names(read_dir_iter);
        assert_eq!(
            &dir_entry_names,
            &["visible_object_key".to_string()],
            "dir must contain file named visible_object_key"
        );
    }
    // if configured so, stat before read
    if stat_before_read {
        let m = metadata(test_session.mount_path().join("visible_object_key")).unwrap();
        assert!(m.file_type().is_file());
        assert_eq!(m.size(), visible_object_props.1 as u64);
    }

    // Read file once
    let mut fh1 = File::options()
        .read(true)
        .open(test_session.mount_path().join(visible_object.0))
        .unwrap();
    let mut read_buffer = Default::default();
    fh1.read_to_end(&mut read_buffer).unwrap();
    assert_eq!(read_buffer, visible_object.1);

    // We can read from a file more than once at the same time.
    let mut fh2 = File::options()
        .read(true)
        .open(test_session.mount_path().join(visible_object.0))
        .unwrap();
    read_buffer.clear();
    fh2.read_to_end(&mut read_buffer).unwrap();
    assert_eq!(read_buffer, visible_object.1);

    // File missing in the manifest must not exist
    let e = File::options()
        .read(true)
        .open(test_session.mount_path().join(invisible_object.0))
        .expect_err("invisible_object_key must not exist");
    assert_eq!(e.kind(), ErrorKind::NotFound);
}

// fn test_manifest_forbidden_operations() // including wrong open flags
// fn test_read_manifest_wrong_etag()
// fn test_read_manifest_wrong_size()
// fn test_read_manifest_missing_etag()
// fn test_read_manifest_missing_size()

fn manifest_test_session_config(db_path: &Path) -> TestSessionConfig {
    TestSessionConfig {
        filesystem_config: S3FilesystemConfig {
            manifest_db_path: Some(db_path.to_path_buf()),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn create_dummy_manifest<T: AsRef<str>>(s3_keys: &[T], file_size: usize) -> (TempDir, PathBuf) {
    let db_entries: Vec<_> = s3_keys
        .iter()
        .map(|key| ManifestEntry::File {
            full_key: key.as_ref().to_string(),
            etag: "".to_owned(),
            size: file_size,
        })
        .collect();

    create_manifest(&db_entries)
}

fn put_dummy_objects<T: AsRef<str>>(test_client: &dyn TestClient, manifest_keys: &[T], excluded_keys: &[T]) {
    for name in manifest_keys.iter().chain(excluded_keys.iter()) {
        let content = vec![b'0'; 1024];
        test_client.put_object(name.as_ref(), &content).unwrap();
    }
}

fn create_manifest(db_entries: &[ManifestEntry]) -> (TempDir, PathBuf) {
    let db_dir = tempfile::tempdir().unwrap();
    let db_path = db_dir.path().join("s3_keys.db3");

    create_db_from_slice(&db_path, db_entries).expect("db must be created");

    (db_dir, db_path)
}

#[cfg(feature = "s3_tests")]
async fn put_object(
    sdk_client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    key: &str,
    content: Vec<u8>,
) -> (String, usize) {
    use aws_sdk_s3::primitives::ByteStream;

    let full_key = format!("{}{}", prefix, key);
    sdk_client
        .put_object()
        .bucket(bucket)
        .key(&full_key)
        .body(ByteStream::from(content))
        .send()
        .await
        .expect("put object must succeed");

    let head_resp = sdk_client
        .head_object()
        .bucket(bucket)
        .key(&full_key)
        .send()
        .await
        .expect("head object must succeed");

    let size = head_resp.content_length().unwrap() as usize;
    (head_resp.e_tag.unwrap(), size)
}
