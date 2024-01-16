// These tests all run the main binary and so expect to be able to reach S3
#![cfg(feature = "s3_tests")]

use assert_cmd::prelude::*;
#[cfg(all(feature = "sse_kms", not(feature = "s3express_tests")))]
use aws_sdk_s3::config::Credentials;
#[cfg(not(feature = "s3express_tests"))]
use aws_sdk_sts::config::Region;
#[cfg(all(feature = "sse_kms", not(feature = "s3express_tests")))]
use regex::Regex;

use std::fs::{self};
use std::io::{BufRead, BufReader};
use std::process::Stdio;
#[cfg(all(feature = "sse_kms", not(feature = "s3express_tests")))]
use std::{fs::File, io::Read, io::Write};
use std::{path::Path, path::PathBuf, process::Command};
use test_case::test_case;

use crate::common::fuse::read_dir_to_entry_names;
#[cfg(not(feature = "s3express_tests"))]
use crate::common::s3::get_subsession_iam_role;
use crate::common::s3::{create_objects, get_test_bucket_and_prefix, get_test_bucket_forbidden, get_test_region};
#[cfg(all(feature = "sse_kms", not(feature = "s3express_tests")))]
use crate::common::{fuse, get_scoped_down_credentials, s3::get_test_kms_key_id, s3::tokio_block_on};
#[cfg(all(feature = "sse_kms", not(feature = "s3express_tests")))]
use mountpoint_s3_client::config::ServerSideEncryption;

const MAX_WAIT_DURATION: std::time::Duration = std::time::Duration::from_secs(10);

fn wait_for_child_to_exit(child: &mut std::process::Child) -> std::process::ExitStatus {
    let st = std::time::Instant::now();

    loop {
        if st.elapsed() > MAX_WAIT_DURATION {
            panic!("wait for result timeout")
        }
        match child.try_wait().expect("unable to wait for result") {
            Some(result) => break result,
            None => std::thread::sleep(std::time::Duration::from_millis(100)),
        }
    }
}

fn wait_for_a_mount(mount_point: &Path) {
    let st = std::time::Instant::now();

    loop {
        if st.elapsed() > MAX_WAIT_DURATION {
            panic!("wait for result timeout")
        }
        if mount_exists("mountpoint-s3", mount_point.to_str().unwrap()) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

#[test]
fn run_in_background() -> Result<(), Box<dyn std::error::Error>> {
    let (bucket, prefix) = get_test_bucket_and_prefix("test_run_in_background");
    let region = get_test_region();
    let mount_point = assert_fs::TempDir::new()?;

    let mut cmd = Command::cargo_bin("mount-s3")?;
    let mut child = cmd
        .arg(&bucket)
        .arg(mount_point.path())
        .arg(format!("--prefix={prefix}"))
        .arg("--auto-unmount")
        .arg(format!("--region={region}"))
        .spawn()
        .expect("unable to spawn child");

    let exit_status = wait_for_child_to_exit(&mut child);

    // verify mount status and mount entry
    assert!(exit_status.success());
    assert!(mount_exists("mountpoint-s3", mount_point.path().to_str().unwrap()));

    test_read_files(&bucket, &prefix, &region, &mount_point.to_path_buf());

    Ok(())
}

#[test]
fn run_in_background_region_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let (bucket, prefix) = get_test_bucket_and_prefix("test_run_in_background_region_from_env");
    let region = get_test_region();
    let mount_point = assert_fs::TempDir::new()?;

    let mut cmd = Command::cargo_bin("mount-s3")?;
    let mut child = cmd
        .arg(&bucket)
        .arg(mount_point.path())
        .arg(format!("--prefix={prefix}"))
        .arg("--auto-unmount")
        .env("AWS_REGION", region.clone())
        .spawn()
        .expect("unable to spawn child");

    let exit_status = wait_for_child_to_exit(&mut child);

    // verify mount status and mount entry
    assert!(exit_status.success());
    assert!(mount_exists("mountpoint-s3", mount_point.path().to_str().unwrap()));

    test_read_files(&bucket, &prefix, &region, &mount_point.to_path_buf());

    Ok(())
}

#[test]
// Automatic region resolution doesn't work with S3 Express One Zone
#[cfg(not(feature = "s3express_tests"))]
fn run_in_background_automatic_region_resolution() -> Result<(), Box<dyn std::error::Error>> {
    let (bucket, prefix) = get_test_bucket_and_prefix("test_run_in_background_automatic_region_resolution");
    let region = get_test_region();
    let mount_point = assert_fs::TempDir::new()?;

    let mut cmd = Command::cargo_bin("mount-s3")?;
    let mut child = cmd
        .arg(&bucket)
        .arg(mount_point.path())
        .arg(format!("--prefix={prefix}"))
        .arg("--auto-unmount")
        .spawn()
        .expect("unable to spawn child");

    let exit_status = wait_for_child_to_exit(&mut child);

    // verify mount status and mount entry
    assert!(exit_status.success());
    assert!(mount_exists("mountpoint-s3", mount_point.path().to_str().unwrap()));

    test_read_files(&bucket, &prefix, &region, &mount_point.to_path_buf());

    Ok(())
}

#[test]
fn run_in_foreground() -> Result<(), Box<dyn std::error::Error>> {
    let (bucket, prefix) = get_test_bucket_and_prefix("test_run_in_foreground");
    let region = get_test_region();
    let mount_point = assert_fs::TempDir::new()?;

    let mut cmd = Command::cargo_bin("mount-s3")?;
    let mut child = cmd
        .arg(&bucket)
        .arg(mount_point.path())
        .arg(format!("--prefix={prefix}"))
        .arg("--auto-unmount")
        .arg("--foreground")
        .arg(format!("--region={region}"))
        .spawn()
        .expect("unable to spawn child");

    wait_for_a_mount(&mount_point);

    // verify that process is still alive
    let child_status = child.try_wait().unwrap();
    assert_eq!(None, child_status);

    assert!(mount_exists("mountpoint-s3", mount_point.path().to_str().unwrap()));

    test_read_files(&bucket, &prefix, &region, &mount_point.to_path_buf());

    Ok(())
}

#[test]
fn run_in_background_fail_on_mount() -> Result<(), Box<dyn std::error::Error>> {
    // the mount would fail from error 403 on HeadBucket
    let bucket = get_test_bucket_forbidden();
    let mount_point = assert_fs::TempDir::new()?;

    let mut cmd = Command::cargo_bin("mount-s3")?;
    let mut child = cmd
        .arg(&bucket)
        .arg(mount_point.path())
        .arg("--auto-unmount")
        .spawn()
        .expect("unable to spawn child");

    let exit_status = wait_for_child_to_exit(&mut child);

    // verify mount status and mount entry
    assert!(!exit_status.success());
    assert!(!mount_exists("mountpoint-s3", mount_point.path().to_str().unwrap()));

    Ok(())
}

#[test]
fn run_in_foreground_fail_on_mount() -> Result<(), Box<dyn std::error::Error>> {
    // the mount would fail from error 403 on HeadBucket
    let bucket = get_test_bucket_forbidden();
    let mount_point = assert_fs::TempDir::new()?;

    let mut cmd = Command::cargo_bin("mount-s3")?;
    let mut child = cmd
        .arg(&bucket)
        .arg(mount_point.path())
        .arg("--auto-unmount")
        .arg("--foreground")
        .spawn()
        .expect("unable to spawn child");

    let exit_status = wait_for_child_to_exit(&mut child);

    // verify mount status and mount entry
    assert!(!exit_status.success());
    assert!(!mount_exists("mountpoint-s3", mount_point.path().to_str().unwrap()));

    Ok(())
}

#[test]
fn run_fail_on_duplicate_mount() -> Result<(), Box<dyn std::error::Error>> {
    let (bucket, prefix) = get_test_bucket_and_prefix("run_fail_on_duplicate_mount");
    let mount_point = assert_fs::TempDir::new()?;
    let region = get_test_region();

    let mut cmd = Command::cargo_bin("mount-s3")?;
    let mut first_mount = cmd
        .arg(&bucket)
        .arg(mount_point.path())
        .arg(format!("--prefix={prefix}"))
        .arg("--auto-unmount")
        .arg(format!("--region={region}"))
        .spawn()
        .expect("unable to spawn child");

    let st = std::time::Instant::now();

    let exit_status = loop {
        if st.elapsed() > MAX_WAIT_DURATION {
            panic!("wait for result timeout")
        }
        match first_mount.try_wait().expect("unable to wait for result") {
            Some(result) => break result,
            None => std::thread::sleep(std::time::Duration::from_millis(100)),
        }
    };

    // verify mount status and mount entry
    assert!(exit_status.success());
    assert!(mount_exists("mountpoint-s3", mount_point.path().to_str().unwrap()));

    let mut cmd = Command::cargo_bin("mount-s3")?;
    let mut second_mount = cmd
        .arg(&bucket)
        .arg(mount_point.path())
        .arg(format!("--prefix={prefix}"))
        .arg("--auto-unmount")
        .spawn()
        .expect("unable to spawn child");

    let st = std::time::Instant::now();

    let exit_status = loop {
        if st.elapsed() > MAX_WAIT_DURATION {
            panic!("wait for result timeout")
        }
        match second_mount.try_wait().expect("unable to wait for result") {
            Some(result) => break result,
            None => std::thread::sleep(std::time::Duration::from_millis(100)),
        }
    };

    // verify mount status
    assert!(!exit_status.success());

    Ok(())
}

#[test]
fn mount_readonly() -> Result<(), Box<dyn std::error::Error>> {
    let (bucket, prefix) = get_test_bucket_and_prefix("test_mount_readonly");
    let mount_point = assert_fs::TempDir::new()?;
    let region = get_test_region();

    let mut cmd = Command::cargo_bin("mount-s3")?;
    let mut child = cmd
        .arg(&bucket)
        .arg(mount_point.path())
        .arg(format!("--prefix={prefix}"))
        .arg("--auto-unmount")
        .arg("--foreground")
        .arg("--read-only")
        .arg(format!("--region={region}"))
        .spawn()
        .expect("unable to spawn child");

    wait_for_a_mount(&mount_point);

    // verify that process is still alive
    let child_status = child.try_wait().unwrap();
    assert_eq!(None, child_status);

    let mount_line =
        get_mount_from_source_and_mountpoint("mountpoint-s3", mount_point.path().to_str().unwrap()).unwrap();

    // mount entry looks like
    // /dev/nvme0n1p2 on /boot type ext4 (rw,relatime)
    let mount_opts_str = mount_line.split_whitespace().last().unwrap();
    let mount_opts: Vec<&str> = mount_opts_str.trim_matches(&['(', ')'] as &[_]).split(',').collect();
    assert!(mount_opts.contains(&"ro"));

    Ok(())
}

#[test_case(true)]
#[test_case(false)]
fn mount_allow_delete(allow_delete: bool) -> Result<(), Box<dyn std::error::Error>> {
    let (bucket, prefix) = get_test_bucket_and_prefix("mount_allow_delete");
    let mount_point = assert_fs::TempDir::new()?;
    let region = get_test_region();

    let mut cmd = Command::cargo_bin("mount-s3")?;
    cmd.arg(&bucket)
        .arg(mount_point.path())
        .arg(format!("--prefix={prefix}"))
        .arg("--auto-unmount")
        .arg(format!("--region={region}"));
    if allow_delete {
        cmd.arg("--allow-delete");
    }
    let mut child = cmd.spawn().expect("unable to spawn child");

    let exit_status = wait_for_child_to_exit(&mut child);

    // verify mount status and mount entry
    assert!(exit_status.success());
    assert!(mount_exists("mountpoint-s3", mount_point.path().to_str().unwrap()));

    // create and try to delete an object
    create_objects(&bucket, &prefix, &region, "file.txt", b"hello world");

    let result = fs::remove_file(mount_point.path().join("file.txt"));
    if allow_delete {
        result.expect("remove file should succeed when --allow_delete is set");
    } else {
        result.expect_err("remove file should fail when --allow_delete is not set");
    }

    Ok(())
}

#[test]
// S3 Express One Zone doesn't support scoped credentials
#[cfg(not(feature = "s3express_tests"))]
fn mount_scoped_credentials() -> Result<(), Box<dyn std::error::Error>> {
    let (bucket, prefix) = get_test_bucket_and_prefix("mount_allow_delete");
    let subprefix = format!("{prefix}sub/");
    let mount_point = assert_fs::TempDir::new()?;
    let region = get_test_region();
    let subsession_role = get_subsession_iam_role();

    // Get scoped down credentials to the subprefix
    let policy = r#"{"Statement": [
        {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"], "Resource": "arn:aws:s3:::__BUCKET__/__PREFIX__*"},
        {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::__BUCKET__", "Condition": {"StringLike": {"s3:prefix": "__PREFIX__*"}}}
    ]}"#;
    let policy = policy.replace("__BUCKET__", &bucket).replace("__PREFIX__", &subprefix);
    let config = tokio_block_on(aws_config::from_env().region(Region::new(get_test_region())).load());
    let sts_client = aws_sdk_sts::Client::new(&config);
    let credentials = tokio_block_on(
        sts_client
            .assume_role()
            .role_arn(subsession_role)
            .role_session_name("test_scoped_credentials")
            .policy(policy)
            .send(),
    )
    .unwrap();
    let credentials = credentials.credentials().unwrap();

    // First try without the subprefix -- mount should fail as we don't have permissions on it
    let mut cmd = Command::cargo_bin("mount-s3")?;
    let mut child = cmd
        .arg(&bucket)
        .arg(mount_point.path())
        .arg(format!("--prefix={prefix}"))
        .arg("--auto-unmount")
        .arg(format!("--region={region}"))
        .env("AWS_ACCESS_KEY_ID", credentials.access_key_id().unwrap())
        .env("AWS_SECRET_ACCESS_KEY", credentials.secret_access_key().unwrap())
        .env("AWS_SESSION_TOKEN", credentials.session_token().unwrap())
        .spawn()
        .expect("unable to spawn child");

    let st = std::time::Instant::now();
    let exit_status = loop {
        if st.elapsed() > MAX_WAIT_DURATION {
            panic!("wait for result timeout")
        }
        match child.try_wait().expect("unable to wait for result") {
            Some(result) => break result,
            None => std::thread::sleep(std::time::Duration::from_millis(100)),
        }
    };

    // verify mount status and mount entry
    assert!(!exit_status.success());
    assert!(!mount_exists("mountpoint-s3", mount_point.path().to_str().unwrap()));

    // Now try with the subprefix -- mount should work since we have the right permissions
    let mut cmd = Command::cargo_bin("mount-s3")?;
    let mut child = cmd
        .arg(&bucket)
        .arg(mount_point.path())
        .arg(format!("--prefix={subprefix}"))
        .arg("--auto-unmount")
        .arg(format!("--region={region}"))
        .env("AWS_ACCESS_KEY_ID", credentials.access_key_id().unwrap())
        .env("AWS_SECRET_ACCESS_KEY", credentials.secret_access_key().unwrap())
        .env("AWS_SESSION_TOKEN", credentials.session_token().unwrap())
        .spawn()
        .expect("unable to spawn child");

    let st = std::time::Instant::now();
    let exit_status = loop {
        if st.elapsed() > MAX_WAIT_DURATION {
            panic!("wait for result timeout")
        }
        match child.try_wait().expect("unable to wait for result") {
            Some(result) => break result,
            None => std::thread::sleep(std::time::Duration::from_millis(100)),
        }
    };

    // verify mount status and mount entry
    assert!(exit_status.success());
    assert!(mount_exists("mountpoint-s3", mount_point.path().to_str().unwrap()));

    test_read_files(&bucket, &subprefix, &region, &mount_point.to_path_buf());

    Ok(())
}

fn test_read_files(bucket: &str, prefix: &str, region: &str, mount_point: &PathBuf) {
    // create objects for test
    create_objects(bucket, prefix, region, "file1.txt", b"hello world");
    create_objects(bucket, prefix, region, "dir/file2.txt", b"hello world");

    // verify readdir works on mount point
    let read_dir_iter = fs::read_dir(mount_point).unwrap();
    let dir_entry_names = read_dir_to_entry_names(read_dir_iter);
    assert_eq!(dir_entry_names, vec!["dir", "file1.txt"]);

    // verify readdir works
    let read_dir_iter = fs::read_dir(mount_point.join("dir")).unwrap();
    let dir_entry_names = read_dir_to_entry_names(read_dir_iter);
    assert_eq!(dir_entry_names, vec!["file2.txt"]);

    // verify read file works
    let file_content = fs::read_to_string(mount_point.as_path().join("file1.txt")).unwrap();
    assert_eq!(file_content, "hello world");

    let file_content = fs::read_to_string(mount_point.as_path().join("dir/file2.txt")).unwrap();
    assert_eq!(file_content, "hello world");
}

fn mount_exists(source: &str, mount_point: &str) -> bool {
    get_mount_from_source_and_mountpoint(source, mount_point).is_some()
}

/// Read all mount records in the system and return the line that matches given arguments.
/// # Arguments
///
/// * `source` - name of the file system.
/// * `mount_point` - path to the mount point.
fn get_mount_from_source_and_mountpoint(source: &str, mount_point: &str) -> Option<String> {
    // macOS wrap its temp directory under /private but it's not visible to users
    #[cfg(target_os = "macos")]
    let mount_point = format!("/private{}", mount_point);

    let mut cmd = Command::new("mount");
    #[cfg(target_os = "linux")]
    cmd.arg("-l");
    let mut cmd = cmd.stdout(Stdio::piped()).spawn().expect("Unable to spawn mount tool");

    let stdout = cmd.stdout.as_mut().unwrap();
    let stdout_reader = BufReader::new(stdout);
    let stdout_lines = stdout_reader.lines();

    for line in stdout_lines.flatten() {
        let str: Vec<&str> = line.split_whitespace().collect();
        let source_rec = str[0];
        let mount_point_rec = str[2];
        if source_rec == source && mount_point_rec == mount_point {
            return Some(line);
        }
    }
    None
}

#[cfg(all(feature = "sse_kms", not(feature = "s3express_tests")))]
struct ChildProcess(std::process::Child);

#[cfg(all(feature = "sse_kms", not(feature = "s3express_tests")))]
impl Drop for ChildProcess {
    fn drop(&mut self) {
        if !self
            .0
            .try_wait()
            .expect("unable to get child's return status")
            .is_some()
        {
            self.0.kill().expect("cannot kill mountpoint");
            wait_for_child_to_exit(&mut self.0);
        }
        if let Some(logs) = self.stdout_utf8() {
            println!("command logs: [{logs}]");
        }
    }
}

#[cfg(all(feature = "sse_kms", not(feature = "s3express_tests")))]
impl ChildProcess {
    fn stdout_utf8(&mut self) -> Option<String> {
        let stdout = self.0.stdout.take();
        if !stdout.is_some() {
            return None;
        }
        let mut buf = Vec::new();
        stdout
            .unwrap()
            .read_to_end(&mut buf)
            .expect("failed to read mountpoint log from pipe");
        Some(String::from_utf8(buf).expect("mountpoint log is not a valid UTF-8"))
    }
}

#[cfg(all(feature = "sse_kms", not(feature = "s3express_tests")))]
fn mount_and_check_log<F: FnOnce(&Path)>(
    bucket: &str,
    prefix: &str,
    key_id: &str,
    io_fun: F,
    expected_log_line: Regex,
    credentials: Option<Credentials>,
) {
    let mount_point = assert_fs::TempDir::new().expect("can not create a mount dir");
    let region = get_test_region();
    let mut cmd = Command::cargo_bin("mount-s3").expect("can not locate mount-s3 binary");
    cmd.stdout(Stdio::piped())
        .arg(bucket)
        .arg(mount_point.path())
        .arg(format!("--region={region}"))
        .arg(format!("--prefix={prefix}"))
        .arg("--server-side-encryption=aws:kms:dsse")
        .arg(format!("--sse-kms-key-id={key_id}"))
        .arg("--auto-unmount")
        .arg("--foreground");
    if let Some(maybe_creds) = credentials {
        cmd.env("AWS_ACCESS_KEY_ID", maybe_creds.access_key_id())
            .env("AWS_SECRET_ACCESS_KEY", maybe_creds.secret_access_key())
            .env("AWS_SESSION_TOKEN", maybe_creds.session_token().unwrap());
    }
    let mut child = ChildProcess(cmd.spawn().expect("unable to spawn child"));
    wait_for_a_mount(&mount_point);

    io_fun(mount_point.path());

    std::thread::sleep(std::time::Duration::from_millis(500)); // wait for logs
    child.0.kill().expect("cannot kill mountpoint");
    let log = child.stdout_utf8().expect("should be able to read child's stdout");
    for line in log.lines() {
        if expected_log_line.is_match(line) {
            return;
        }
    }
    panic!("can not find a matching line in log: [{log}]");
}

#[cfg(all(feature = "sse_kms", not(feature = "s3express_tests")))]
fn erroneous_write(mount_point: &Path) {
    let mut f = File::create(mount_point.join("file.txt")).expect("can not open file for write");
    let data = vec![0xaa; 32];
    let write_result = f.write_all(&data);
    assert!(
        write_result.is_err(),
        "should not be able to write to the file without proper sse"
    );
}

#[cfg(all(feature = "sse_kms", not(feature = "s3express_tests")))]
#[test]
fn write_with_inexistent_key() {
    let expected_log_line =
        Regex::new(r"^.*WARN.*KMS.NotFoundException.*Invalid keyId \\'SOME_INVALID_KEY\\'.*$").unwrap();
    let (bucket, prefix) = get_test_bucket_and_prefix("mount_and_check_log");

    mount_and_check_log(
        &bucket,
        &prefix,
        "SOME_INVALID_KEY",
        erroneous_write,
        expected_log_line,
        None,
    );
}

#[cfg(all(feature = "sse_kms", not(feature = "s3express_tests")))]
#[test]
fn write_with_no_permissions_for_a_key() {
    let sse_key = get_test_kms_key_id();
    let log_line_pattern = format!("^.*WARN.*User: [^ ]* is not authorized to perform: kms:GenerateDataKey on resource: {sse_key} because no session policy allows the kms:GenerateDataKey action.*$");
    let expected_log_line = Regex::new(&log_line_pattern).unwrap();
    let policy_with_no_kms_perms = r#"{"Statement": [
        {"Effect": "Allow", "Action": ["s3:*"], "Resource": "*"}
    ]}"#;
    let credentials = tokio_block_on(get_scoped_down_credentials(policy_with_no_kms_perms));
    let (bucket, prefix) = get_test_bucket_and_prefix("mount_and_check_log");

    mount_and_check_log(
        &bucket,
        &prefix,
        &sse_key,
        erroneous_write,
        expected_log_line,
        Some(credentials),
    );
}

#[cfg(all(feature = "sse_kms", not(feature = "s3express_tests")))]
#[test]
fn read_with_no_permissions_for_a_key() {
    // create file
    use mountpoint_s3_client::types::PutObjectParams;
    let (bucket, prefix) = get_test_bucket_and_prefix("mount_and_check_log");
    let mut test_client = fuse::s3_session::create_test_client(&get_test_region(), &bucket, &prefix);
    let encrypted_object = "encrypted_with_kms";
    let unencrypted_object = "unencrypted_with_s3_keys";
    let data = vec![0xaa; 32];
    let sse_key = get_test_kms_key_id();
    test_client
        .put_object_params(
            encrypted_object,
            &data,
            PutObjectParams::new().server_side_encryption(ServerSideEncryption::DualLayerKms {
                key_id: Some(sse_key.clone()),
            }),
        )
        .expect("failed to create an object in S3");
    test_client
        .put_object(unencrypted_object, &data)
        .expect("failed to create an object in S3");

    // try to read it
    let log_line_pattern = format!("^.*WARN.*{encrypted_object}.*read failed: get request failed: get object request failed: Client error: Forbidden: User: .* is not authorized to perform: kms:Decrypt on resource: {sse_key} because no session policy allows the kms:Decrypt action.*$");
    let expected_log_line = Regex::new(&log_line_pattern).unwrap();
    let policy_with_no_kms_perms = r#"{"Statement": [
        {"Effect": "Allow", "Action": ["s3:*"], "Resource": "*"}
    ]}"#;
    let credentials = tokio_block_on(get_scoped_down_credentials(policy_with_no_kms_perms));
    let io_fun = |mount_point: &Path| {
        let encrypted_object = mount_point.join(encrypted_object);
        let mut data = Vec::new();
        let mut f = File::open(encrypted_object).expect("can not open file for read");
        let read_result = f.read_to_end(&mut data);
        assert!(
            read_result.is_err(),
            "should not be able to read a kms-encrypted file without kms permissions"
        );

        let unencrypted_object = mount_point.join(unencrypted_object);
        let mut f = File::open(unencrypted_object).expect("can not open file for read");
        let read_result = f.read_to_end(&mut data);
        assert!(
            read_result.is_ok(),
            "should not able to read a default-encrypted file after the first read failure"
        );
    };

    mount_and_check_log(&bucket, &prefix, &sse_key, io_fun, expected_log_line, Some(credentials));
}
