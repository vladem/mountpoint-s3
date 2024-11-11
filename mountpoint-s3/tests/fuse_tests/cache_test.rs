use crate::common::fuse;
use crate::common::metrics::{get_counter_value, TestRecorder};
use crate::common::s3::{get_express_cache_bucket, get_test_bucket_and_prefix};
use mountpoint_s3::data_cache::{DataCache, DiskDataCache, DiskDataCacheConfig, ExpressDataCache};
use mountpoint_s3_client::S3CrtClient;
use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaChaRng;
use rusty_fork::rusty_fork_test;
use std::fs;
use std::path::PathBuf;
use std::thread::sleep;
use std::time::Duration;

// The following tests use global metrics registry of the process,
// so a separate process is spawned for each of the them.
rusty_fork_test! {
    #[test]
    fn express_cache_write_read_non_ascii() {
        cache_write_read_base(
            "£",
            100,
            1024,
            express_cache_factory,
            "express_cache_write_read_non_ascii",
        );
    }
}

rusty_fork_test! {
    #[test]
    fn express_cache_write_read_long_key() {
        cache_write_read_base(
            "key",
            1024,
            1024,
            express_cache_factory,
            "express_cache_write_read_long_key",
        );
    }
}

rusty_fork_test! {
    #[test]
    fn express_cache_write_read_big_file() {
        cache_write_read_base(
            "key",
            100,
            1024 * 1024,
            express_cache_factory,
            "express_cache_write_read_big_file",
        );
    }
}

rusty_fork_test! {
    #[test]
    fn disk_cache_write_read_simple() {
        let cache_dir = tempfile::tempdir().unwrap();
        cache_write_read_base(
            "key",
            100,
            1024,
            disk_cache_factory(cache_dir.path().to_owned()),
            "disk_cache_write_read_simple",
        );
    }
}

fn cache_write_read_base<Cache, CacheFactory>(
    key_suffix: &str,
    key_size: usize,
    object_size: usize,
    cache_factory: CacheFactory,
    test_name: &str,
) where
    Cache: DataCache + Send + Sync + 'static,
    CacheFactory: FnOnce(S3CrtClient, u64) -> Cache,
{
    // set up metrics
    let recorder = TestRecorder::default();
    metrics::set_global_recorder(recorder.clone()).unwrap();
    const SERVED_FROM_CACHE_METRIC_NAME: &str = "prefetch.blocks_served_from_cache";
    const STORED_TO_CACHE_METRIC_NAME: &str = "prefetch.blocks_stored_to_cache";

    // mount a bucket
    const BLOCK_SIZE: u64 = 1024 * 1024;
    let (_, prefix) = get_test_bucket_and_prefix(test_name);
    let (mount_point, _session, _client) =
        fuse::s3_session::new_with_cache_factory(prefix.clone(), cache_factory, Default::default(), BLOCK_SIZE);

    // write an object, no caching happens yet
    let key = get_object_key(&prefix, key_suffix, key_size);
    let path = mount_point.path().join(&key);
    let written = random_binary_data(object_size);
    fs::write(&path, &written).expect("write should succeed");

    // first read should be from the source bucket and be cached
    let read = fs::read(&path).expect("read should succeed");
    assert_eq!(read, written);

    // cache writes are async, wait for that to happen
    const MAX_WAIT_DURATION: std::time::Duration = std::time::Duration::from_secs(10);
    let st = std::time::Instant::now();
    loop {
        if st.elapsed() > MAX_WAIT_DURATION {
            panic!("timeout on waiting for data being stored to the cache")
        }
        if get_counter_value(&recorder, STORED_TO_CACHE_METRIC_NAME) == 1 {
            break;
        }
        sleep(Duration::from_millis(100));
    }

    // second read should be from the cache
    let prev_counter_value = get_counter_value(&recorder, SERVED_FROM_CACHE_METRIC_NAME);
    let read = fs::read(&path).expect("read from the cache should succeed");
    assert_eq!(read, written);

    // ensure data was served from the cache
    assert!(
        get_counter_value(&recorder, SERVED_FROM_CACHE_METRIC_NAME) > prev_counter_value,
        "some blocks should be served from the cache",
    );
}

fn express_cache_factory(client: S3CrtClient, block_size: u64) -> ExpressDataCache<S3CrtClient> {
    let express_bucket_name = get_express_cache_bucket();
    ExpressDataCache::new(&express_bucket_name, client, &express_bucket_name, block_size)
}

fn disk_cache_factory(cache_dir: PathBuf) -> impl FnOnce(S3CrtClient, u64) -> DiskDataCache {
    move |_, block_size| {
        let cache_config = DiskDataCacheConfig {
            block_size,
            limit: Default::default(),
        };
        DiskDataCache::new(cache_dir, cache_config)
    }
}

fn random_binary_data(size_in_bytes: usize) -> Vec<u8> {
    let seed = rand::thread_rng().gen();
    let mut rng = ChaChaRng::seed_from_u64(seed);
    let mut data = vec![0; size_in_bytes];
    rng.fill_bytes(&mut data);
    data
}

// Creates a random key which has a size of at least `min_size_in_bytes`
fn get_object_key(key_prefix: &str, key_suffix: &str, min_size_in_bytes: usize) -> String {
    let random_suffix: u64 = rand::thread_rng().gen();
    let last_key_part = format!("{key_suffix}{random_suffix}"); // part of the key after all the "/"
    let full_key = format!("{key_prefix}{last_key_part}");
    let full_key_size = full_key.as_bytes().len();
    let padding_size = min_size_in_bytes.saturating_sub(full_key_size);
    let padding = "0".repeat(padding_size);
    format!("{last_key_part}{padding}")
}
