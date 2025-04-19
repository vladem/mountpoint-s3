use crate::common::fuse::{self, read_dir_to_entry_names, TestClient, TestSessionConfig};
#[cfg(feature = "s3_tests")]
use crate::common::s3::{get_test_bucket_and_prefix, get_test_region, get_test_sdk_client};
use mountpoint_s3_fs::manifest::{create_db, DbEntry, ManifestError, ManifestWarning};
use mountpoint_s3_fs::S3FilesystemConfig;
use rusqlite::{Connection, Row};
use std::fs::{self, metadata};
use std::io::ErrorKind;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
#[cfg(feature = "s3_tests")]
use std::{fs::File, io::Read};
use tempfile::TempDir;
use test_case::test_case;

const DUMMY_ETAG: &str = "\"3bebe4037c8f040e0e573e191d34b2c6\"";
const DUMMY_SIZE: usize = 1024;

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
    let (_tmp_dir, db_path, _) = create_dummy_manifest(manifest_keys, 0);
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

    let (_tmp_dir, db_path, _) = create_dummy_manifest(&manifest_keys, 0);
    let test_session = fuse::mock_session::new("", manifest_test_session_config(&db_path));
    put_dummy_objects(test_session.client(), &manifest_keys, excluded_keys);

    let read_dir_iter = fs::read_dir(test_session.mount_path().join(directory_to_list)).unwrap();
    let dir_entry_names = read_dir_to_entry_names(read_dir_iter);
    assert_eq!(dir_entry_names, expected_children, "readdir test failed");
}

#[test_case(Some("dummy_etag"), None; "missing size")]
#[test_case(None, Some(1); "missing etag")]
fn test_readdir_manifest_missing_metadata(etag: Option<&str>, size: Option<usize>) {
    let key = "key";
    let (_tmp_dir, db_path, _) = create_dummy_manifest::<&str>(&[], 0);
    let test_session = fuse::mock_session::new("", manifest_test_session_config(&db_path));
    insert_entries(&db_path, &[(key, "", etag, size)]).expect("insert invalid row must succeed");

    let mut read_dir_iter = fs::read_dir(test_session.mount_path()).unwrap();
    let e = read_dir_iter
        .next()
        .expect("iterator not empty")
        .expect_err("first item is an error");
    assert_eq!(e.raw_os_error().expect("must be an error"), libc::EIO);
    assert!(read_dir_iter.next().is_none(), "no more items in the iterator");
}

#[test]
fn test_lookup_unicode_keys_manifest() {
    // todo: are non UTF-8 keys supported?
    let file_size = 1024;
    let keys = &["مرحبًا", "🇦🇺", "🐈/🦀", "a\0"];
    let excluded_keys = &["こんにちは"];
    let (_tmp_dir, db_path, _) = create_dummy_manifest(keys, file_size);
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

#[test_case(Some("dummy_etag"), None; "missing size")]
#[test_case(None, Some(1); "missing etag")]
fn test_lookup_manifest_missing_metadata(etag: Option<&str>, size: Option<usize>) {
    let key = "key";
    let (_tmp_dir, db_path, _) = create_dummy_manifest::<&str>(&[], 0);
    let test_session = fuse::mock_session::new("", manifest_test_session_config(&db_path));
    insert_entries(&db_path, &[(key, "", etag, size)]).expect("insert invalid row must succeed");

    let e = metadata(test_session.mount_path().join(key)).expect_err("lookup must fail");
    assert_eq!(e.raw_os_error().expect("lookup must fail"), libc::EIO);
}

#[cfg(feature = "s3_tests")]
#[test_case(false, false; "just read")]
#[test_case(true, false; "readdir then read")]
#[test_case(false, true; "stat then read")]
#[tokio::test]
async fn test_basic_read_manifest_s3(readdir_before_read: bool, stat_before_read: bool) {
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
    let (_tmp_dir, db_path, _) = create_manifest(&[DbEntry {
        full_key: format!("{}{}", prefix, visible_object.0),
        etag: Some(visible_object_props.0),
        size: Some(visible_object_props.1),
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

#[cfg(feature = "s3_tests")]
#[test_case(false, true, libc::EIO; "wrong size")]
#[test_case(true, false, libc::ESTALE; "wrong etag")]
#[tokio::test]
async fn test_read_manifest_wrong_metadata(wrong_etag: bool, wrong_size: bool, errno: i32) {
    let object = ("visible_object_key", vec![b'1'; 1024]);
    let (bucket, prefix) = get_test_bucket_and_prefix("test_basic_read_manifest_s3");
    let sdk_client = get_test_sdk_client(&get_test_region()).await;
    let object_props = put_object(&sdk_client, &bucket, &prefix, object.0, object.1.clone()).await;

    let (_tmp_dir, db_path, _) = create_manifest(&[DbEntry {
        full_key: format!("{}{}", prefix, object.0),
        etag: if wrong_etag {
            Some("wrong_etag".to_string())
        } else {
            Some(object_props.0)
        },
        size: if wrong_size { Some(2048) } else { Some(object_props.1) }, // size smaller than actual will result in incomplete response
    }]);
    let test_session =
        fuse::s3_session::new_with_test_client(manifest_test_session_config(&db_path), sdk_client, &bucket, &prefix);

    let mut fh = File::options()
        .read(true)
        .open(test_session.mount_path().join(object.0))
        .unwrap();
    let mut read_buffer = Default::default();
    let e = fh.read_to_end(&mut read_buffer).expect_err("read must fail");
    assert_eq!(e.raw_os_error().expect("read must fail"), errno);
}

#[test_case(&[
    "dir1/a.txt",
    "dir1/dir2/b.txt",
    "dir1/dir2/c.txt",
    "dir1/dir3/dir4/d.txt",
    "e.txt",
]; "simple")]
#[test_case(&[
    "dir1/dir2/b.txt",
    "dir1/a.txt",
    "e.txt",
    "dir1/dir3/dir4/d.txt",
    "dir1/dir2/c.txt",
]; "unsorted")]
fn test_ingest_directories(manifest_keys: &[&str]) {
    let all_expected_entries = &[
        TestDbEntry::directory("dir1", ""),
        TestDbEntry::file("dir1/a.txt", "dir1", DUMMY_ETAG, DUMMY_SIZE),
        TestDbEntry::directory("dir1/dir2", "dir1"),
        TestDbEntry::file("dir1/dir2/b.txt", "dir1/dir2", DUMMY_ETAG, DUMMY_SIZE),
        TestDbEntry::file("dir1/dir2/c.txt", "dir1/dir2", DUMMY_ETAG, DUMMY_SIZE),
        TestDbEntry::directory("dir1/dir3", "dir1"),
        TestDbEntry::directory("dir1/dir3/dir4", "dir1/dir3"),
        TestDbEntry::file("dir1/dir3/dir4/d.txt", "dir1/dir3/dir4", DUMMY_ETAG, DUMMY_SIZE),
        TestDbEntry::file("e.txt", "", DUMMY_ETAG, DUMMY_SIZE),
    ];
    let (_tmp_dir, db_path, _) = create_dummy_manifest(manifest_keys, DUMMY_SIZE);
    let db_entries = select_all(&db_path).expect("must select all objects");
    assert_eq!(&db_entries, all_expected_entries);
}

#[test_case(&[
    "dir1", // must be shadowed
    "dir1/a.txt",
    "dir2/b.txt",
]; "simple")]
#[test_case(&[
    "dir1/a.txt",
    "dir2/b.txt",
    "dir1", // must be shadowed
]; "unsorted")]
fn test_ingest_shadowed(manifest_keys: &[&str]) {
    let all_expected_entries = &[
        TestDbEntry::directory("dir1", ""),
        TestDbEntry::file("dir1/a.txt", "dir1", DUMMY_ETAG, DUMMY_SIZE),
        TestDbEntry::directory("dir2", ""),
        TestDbEntry::file("dir2/b.txt", "dir2", DUMMY_ETAG, DUMMY_SIZE),
    ];
    let (_tmp_dir, db_path, warnings) = create_dummy_manifest(manifest_keys, DUMMY_SIZE);
    let db_entries = select_all(&db_path).expect("must select all objects");
    assert_eq!(&db_entries, all_expected_entries);
    assert_eq!(&warnings, &[ManifestWarning::ShadowedKey("dir1".to_string())]);
}

// #[test]
// fn test_readdir_manifest_empty() {
// }

// fn test_manifest_forbidden_operations() // including wrong open flags
// fn test_fs_creation_no_manifest() // empty_manifest, wrong format

// #[test_case("dir1/./a.txt"; "with .")]
// #[test_case("dir1/../a.txt"; "with ..")]
// #[test_case("dir1//a.txt"; "with //")]
// #[test_case(""; "empty")]
// #[test_case("dir1/"; "ends with /")]
// #[test_case("dir1\0"; "with 0")]
// fn test_ingest_invalid_key(key: &str) {
// }

// #[test]
// fn test_ingest_missing_metadata() {
//     let all_expected_entries = &[
//         TestDbEntry{key: "dir1/a.txt".to_string(), parent_key: "dir1".to_string(), etag: None, size: Some(DUMMY_SIZE)},
//         TestDbEntry{key: "dir1/b.txt".to_string(), parent_key: "dir1".to_string(), etag: Some(DUMMY_ETAG.to_string()), size: None},
//     ];
//     let (_tmp_dir, db_path, warnings) = create_manifest(all_expected_entries);
//     let db_entries = select_all(&db_path).expect("must select all objects");
//     assert!(db_entries.is_empty());
//     assert_eq!(&warnings, &[
//         ManifestWarning::InvalidEntry("dir1/a.txt".to_string()),
//         ManifestWarning::InvalidEntry("dir1/b.txt".to_string()),
//     ]);
// }

fn manifest_test_session_config(db_path: &Path) -> TestSessionConfig {
    TestSessionConfig {
        filesystem_config: S3FilesystemConfig {
            manifest_db_path: Some(db_path.to_path_buf()),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn create_dummy_manifest<T: AsRef<str>>(s3_keys: &[T], file_size: usize) -> (TempDir, PathBuf, Vec<ManifestWarning>) {
    let db_entries: Vec<_> = s3_keys
        .iter()
        .map(|key| DbEntry {
            full_key: key.as_ref().to_string(),
            etag: Some(DUMMY_ETAG.to_string()),
            size: Some(file_size),
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

fn create_manifest(db_entries: &[DbEntry]) -> (TempDir, PathBuf, Vec<ManifestWarning>) {
    let db_dir = tempfile::tempdir().unwrap();
    let db_path = db_dir.path().join("s3_keys.db3");

    let warnings = create_db_from_slice(&db_path, db_entries).expect("db must be created");

    (db_dir, db_path, warnings)
}

fn create_db_from_slice(db_path: &Path, db_entries: &[DbEntry]) -> Result<Vec<ManifestWarning>, ManifestError> {
    let batch_size = 1024;
    create_db(db_path, db_entries.iter().map(|entry| Ok(entry.clone())), batch_size)
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

/// Entry from a db. Compared to [DbEntry] it has a `parent_key` field.
#[derive(Debug, PartialEq)]
struct TestDbEntry {
    key: String,
    parent_key: String,
    etag: Option<String>,
    size: Option<usize>,
}

impl TestDbEntry {
    fn file(key: &str, parent_key: &str, etag: &str, size: usize) -> TestDbEntry {
        Self {
            key: key.to_string(),
            parent_key: parent_key.to_string(),
            etag: Some(etag.to_string()),
            size: Some(size),
        }
    }

    fn directory(key: &str, parent_key: &str) -> TestDbEntry {
        Self {
            key: key.to_string(),
            parent_key: parent_key.to_string(),
            etag: None,
            size: None,
        }
    }

    fn from_row(row: &Row) -> rusqlite::Result<TestDbEntry> {
        Ok(Self {
            key: row.get(0)?,
            parent_key: row.get(1)?,
            etag: row.get(2)?,
            size: row.get(3)?,
        })
    }
}

fn insert_entries(
    manifest_db_path: &Path,
    entries: &[(&str, &str, Option<&str>, Option<usize>)],
) -> rusqlite::Result<()> {
    let conn = Connection::open(manifest_db_path).expect("must connect to a db");
    conn.execute_batch("BEGIN TRANSACTION;")?;
    let mut stmt = conn.prepare("INSERT INTO s3_objects (key, parent_key, etag, size) VALUES (?1, ?2, ?3, ?4)")?;
    for entry in entries {
        stmt.execute(*entry)?;
    }
    conn.execute_batch("COMMIT;")?;
    Ok(())
}

fn select_all(manifest_db_path: &Path) -> rusqlite::Result<Vec<TestDbEntry>> {
    let conn = Connection::open(manifest_db_path).expect("must connect to a db");
    let query = "SELECT key, parent_key, etag, size FROM s3_objects ORDER BY key";
    let mut stmt = conn.prepare(query)?;
    let result: rusqlite::Result<Vec<TestDbEntry>> = stmt.query_map((), TestDbEntry::from_row)?.collect();
    result
}
