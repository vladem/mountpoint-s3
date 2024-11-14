use crate::common::cache::CacheTestWrapper;
use crate::common::fuse::create_fuse_session;
use crate::common::fuse::s3_session::create_crt_client;
use crate::common::s3::{
    get_bucket_owner, get_express_bucket, get_external_express_bucket, get_standard_bucket, get_test_endpoint_config,
    get_test_prefix,
};
use fuser::BackgroundSession;
use mountpoint_s3::data_cache::{DataCache, DiskDataCache, DiskDataCacheConfig, ExpressDataCache};
use mountpoint_s3::prefetch::caching_prefetch;
use mountpoint_s3::S3FilesystemConfig;
use mountpoint_s3_client::config::S3ClientConfig;
use mountpoint_s3_client::S3CrtClient;
use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaChaRng;
use std::fs;
use std::time::Duration;
use tempfile::TempDir;
use test_case::test_case;

const CACHE_BLOCK_SIZE: u64 = 1024 * 1024;
const CLIENT_PART_SIZE: usize = 8 * 1024 * 1024;

/// A test that checks that a corrupted block may not be served from the cache
#[test]
fn express_corrupted_block_read() {
}

/// A test that checks that data is now written to the bucket with the wrong owner
#[test_case(get_express_bucket(), false; "bucket matches the expected owner")]
#[test_case(get_external_express_bucket(), true; "bucket owned by another account")]
fn express_expected_bucket_owner(cache_bucket: String, should_fail: bool) {
    let bucket_owner = get_bucket_owner();
    // Configure the client to enforce the bucket owner
    let client_config = S3ClientConfig::default()
        .part_size(CLIENT_PART_SIZE)
        .endpoint_config(get_test_endpoint_config())
        .read_backpressure(true)
        .initial_read_window(CLIENT_PART_SIZE)
        .bucket_owner(&bucket_owner);
    let client = S3CrtClient::new(client_config).unwrap();

    // Create cache and mount a bucket
    let bucket = get_standard_bucket();
    let prefix = get_test_prefix("express_expected_bucket_owner");
    let cache = ExpressDataCache::new(client.clone(), Default::default(), &bucket, &cache_bucket);
    let cache = CacheTestWrapper::new(cache);
    let (mount_point, _session) = mount_bucket(client, cache.clone(), &bucket, &prefix, Default::default());

    // Write an object, no caching happens yet
    let key = get_object_key(&prefix, "key", 100);
    let path = mount_point.path().join(&key);
    let written = random_binary_data(CACHE_BLOCK_SIZE as usize);
    fs::write(&path, &written).expect("write should succeed");

    // First read should be from the source bucket and be cached
    let put_block_count = cache.put_block_count();
    let read = fs::read(&path).expect("read should succeed");
    assert_eq!(read, written);

    // Cache population is async, wait for it to happen
    cache.wait_for_put(Duration::from_secs(10), put_block_count);

    if should_fail {
        assert_eq!(cache.put_block_fail_count(), cache.put_block_count());
    } else {
        assert_eq!(cache.put_block_fail_count(), 0);
    }
}

#[test_case("key", 100, 1024; "simple")]
#[test_case("£", 100, 1024; "non-ascii key")]
#[test_case("key", 1024, 1024; "long key")]
#[test_case("key", 100, 1024 * 1024; "big file")]
fn express_cache_write_read(key_suffix: &str, key_size: usize, object_size: usize) {
    let client = create_crt_client(CLIENT_PART_SIZE, CLIENT_PART_SIZE);
    let bucket_name = get_standard_bucket();
    let express_bucket_name = get_express_bucket();
    let cache = ExpressDataCache::new(client.clone(), Default::default(), &bucket_name, &express_bucket_name);

    cache_write_read_base(
        client,
        key_suffix,
        key_size,
        object_size,
        cache,
        "express_cache_write_read",
    )
}

#[test_case("key", 100, 1024; "simple")]
#[test_case("£", 100, 1024; "non-ascii key")]
#[test_case("key", 1024, 1024; "long key")]
#[test_case("key", 100, 1024 * 1024; "big file")]
fn disk_cache_write_read(key_suffix: &str, key_size: usize, object_size: usize) {
    let cache_dir = tempfile::tempdir().unwrap();
    let cache_config = DiskDataCacheConfig {
        block_size: CACHE_BLOCK_SIZE,
        limit: Default::default(),
    };
    let cache = DiskDataCache::new(cache_dir.path().to_path_buf(), cache_config);

    let client = create_crt_client(CLIENT_PART_SIZE, CLIENT_PART_SIZE);

    cache_write_read_base(
        client,
        key_suffix,
        key_size,
        object_size,
        cache,
        "disk_cache_write_read",
    );
}

fn cache_write_read_base<Cache>(
    client: S3CrtClient,
    key_suffix: &str,
    key_size: usize,
    object_size: usize,
    cache: Cache,
    test_name: &str,
) where
    Cache: DataCache + Send + Sync + 'static,
{
    let bucket = get_standard_bucket();
    let prefix = get_test_prefix(test_name);

    // Mount a bucket
    let cache = CacheTestWrapper::new(cache);
    let (mount_point, _session) = mount_bucket(client, cache.clone(), &bucket, &prefix, Default::default());

    // Write an object, no caching happens yet
    let key = get_object_key(&prefix, key_suffix, key_size);
    let path = mount_point.path().join(&key);
    let written = random_binary_data(object_size);
    fs::write(&path, &written).expect("write should succeed");
    let put_block_count = cache.put_block_count();
    assert_eq!(put_block_count, 0, "no cache writes should happen yet");

    // First read should be from the source bucket and be cached
    let read = fs::read(&path).expect("read should succeed");
    assert_eq!(read, written);

    // Cache population is async, wait for it to happen
    cache.wait_for_put(Duration::from_secs(10), put_block_count);

    // Second read should be from the cache
    let cache_hits_before_read = cache.get_block_hit_count();
    let read = fs::read(&path).expect("read from the cache should succeed");
    assert_eq!(read, written);
    assert!(
        cache.get_block_hit_count() > cache_hits_before_read,
        "read should result in a cache hit"
    );
}

/// Generates random data of the specified size
fn random_binary_data(size_in_bytes: usize) -> Vec<u8> {
    let seed = rand::thread_rng().gen();
    let mut rng = ChaChaRng::seed_from_u64(seed);
    let mut data = vec![0; size_in_bytes];
    rng.fill_bytes(&mut data);
    data
}

/// Creates a random key which has a size of at least `min_size_in_bytes`
fn get_object_key(key_prefix: &str, key_suffix: &str, min_size_in_bytes: usize) -> String {
    let random_suffix: u64 = rand::thread_rng().gen();
    let last_key_part = format!("{key_suffix}{random_suffix}"); // part of the key after all the "/"
    let full_key = format!("{key_prefix}{last_key_part}");
    let full_key_size = full_key.as_bytes().len();
    let padding_size = min_size_in_bytes.saturating_sub(full_key_size);
    let padding = "0".repeat(padding_size);
    format!("{last_key_part}{padding}")
}

fn mount_bucket<Cache>(
    client: S3CrtClient,
    cache: Cache,
    bucket: &str,
    prefix: &str,
    config: S3FilesystemConfig,
) -> (TempDir, BackgroundSession)
where
    Cache: DataCache + Send + Sync + 'static,
{
    let mount_point = tempfile::tempdir().unwrap();
    let runtime = client.event_loop_group();
    let prefetcher = caching_prefetch(cache, runtime, Default::default());
    let session = create_fuse_session(client, prefetcher, bucket, prefix, mount_point.path(), config);
    (mount_point, session)
}
