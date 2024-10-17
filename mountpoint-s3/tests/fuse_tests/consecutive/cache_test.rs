use rand::Rng;

use crate::common::fuse;

use std::{fs, sync::Arc};

use mountpoint_s3::metrics::{MetricsRecorder, MetricsSink};

#[cfg(all(feature = "s3_tests", feature = "s3express_tests"))]
#[test]
fn express_cache_long_key() {
}

#[cfg(all(feature = "s3_tests", feature = "s3express_tests"))]
#[test]
fn express_cache_non_ascii_key() {
}


#[cfg(all(feature = "s3_tests", feature = "s3express_tests"))]
#[test]
fn express_cache_max_object_size() {
}

#[cfg(all(feature = "s3_tests", feature = "s3express_tests"))]
#[test]
fn express_cache_miss_hit() {
    let metrics_sink = reset_metrics_recorder();
    let session_factory = fuse::s3_session::new_with_express_cache();
    let (mount_point, _session, _) = session_factory("express_cache_test", Default::default());

    let random_suffix: u64 = rand::thread_rng().gen();
    let key = format!("key_{random_suffix}");
    let path = mount_point.path().join(key);
    let content = "content";
    fs::write(&path, content).expect("write should succeed");

    // first read should be from the source bucket
    let read = fs::read_to_string(&path).expect("read should succeed");
    assert_eq!(read, content);
    let metrics_snapshot = metrics_sink.publish_as_hash_map();
    let cache_hits = metrics_snapshot.get("express_data_cache.block_hit").map(|value| *value).unwrap_or(0);
    assert_eq!(cache_hits, 0);

    // second read should be from cache
    let read = fs::read_to_string(&path).expect("read should succeed");
    assert_eq!(read, content);
    let metrics_snapshot = metrics_sink.publish_as_hash_map();
    let cache_hits = metrics_snapshot.get("express_data_cache.block_hit").map(|value| *value).unwrap_or(0);
    assert_eq!(cache_hits, 1);
}

fn reset_metrics_recorder() -> Arc<MetricsSink> {
    let sink = Arc::new(MetricsSink::new());
    let recorder = MetricsRecorder { sink: sink.clone() };
    metrics::set_global_recorder(recorder).unwrap();
    sink
}
