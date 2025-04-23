use std::{
    cmp::max,
    sync::mpsc::{self, TryRecvError},
    time::{Duration, Instant},
};

use crate::common::manifest::{count_all, create_manifest, DUMMY_ETAG, DUMMY_SIZE};
use mountpoint_s3_fs::manifest::DbEntry;
use rusty_fork::rusty_fork_test;
use sysinfo::{MemoryRefreshKind, ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System};

const BATCH_SIZE: usize = 100_000;

// Run tests as separate processes so the RAM consumption is 0 in the start of the test
rusty_fork_test! {
    #[test]
    fn test_ingest_1m_files() {
        let input_keys_num = 1_000_000;
        test_ingest_many_keys_base(|idx| format!("dir1/dir2/key_{}.jpg", idx), input_keys_num, input_keys_num + 2, 30, 128);
    }

    #[test]
    fn test_ingest_1m_dirs() {
        let input_keys_num = 1_000_000;
        test_ingest_many_keys_base(|idx| format!("dir1/dir_{}/key_{}.jpg", idx, idx), input_keys_num, 2 * input_keys_num + 1, 30, 128);
    }
}

fn test_ingest_many_keys_base(
    key_generator: fn(usize) -> String,
    input_keys_num: usize,
    output_keys_num: usize,
    max_duration_s: u64,
    max_mem_usage_mb: u64,
) {
    let (_tmp_dir, db_path, _) =
        monitor_with_timeout(Duration::from_secs(max_duration_s), max_mem_usage_mb, move || {
            create_manifest(
                (0..input_keys_num).map(key_generator).map(|key| {
                    Ok(DbEntry {
                        full_key: key,
                        etag: Some(DUMMY_ETAG.to_string()),
                        size: Some(DUMMY_SIZE),
                    })
                }),
                BATCH_SIZE,
            )
        });

    let actual_output_keys_num = count_all(&db_path).expect("must count all objects");
    assert_eq!(actual_output_keys_num, output_keys_num);
}

// Invokes a function in a separate thread, tracks memory usage and elapsed time, panicking if either of them is over the limit
fn monitor_with_timeout<T, F>(max_duration_s: Duration, max_mem_usage_mb: u64, f: F) -> T
where
    T: Send + 'static,
    F: FnOnce() -> T,
    F: Send + 'static,
{
    let (done_tx, done_rx) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        let val = f();
        done_tx.send(()).expect("unable to send completion signal");
        val
    });

    let start = Instant::now();
    let mut peak_mem_usage_mb = 0;
    let mut system = System::new_with_specifics(RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()));
    while matches!(done_rx.try_recv(), Err(TryRecvError::Empty)) {
        assert!(start.elapsed() < max_duration_s, "timeout after {:?}", max_duration_s);
        let memory_usage_mb = mem(&mut system);
        peak_mem_usage_mb = max(peak_mem_usage_mb, memory_usage_mb);
        assert!(
            memory_usage_mb < max_mem_usage_mb,
            "breached memory usage limit: {} < {}",
            memory_usage_mb,
            max_mem_usage_mb
        );
        std::thread::sleep(std::time::Duration::from_millis(1000));
    }
    println!(
        "elapsed: {:?}, peak mem usage mb: {}",
        start.elapsed(),
        peak_mem_usage_mb
    );

    handle.join().expect("thread must not panic")
}

fn mem(system: &mut System) -> u64 {
    let pid = sysinfo::get_current_pid().expect("must get current pid");

    let remove_dead_processes = true;
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        remove_dead_processes,
        ProcessRefreshKind::everything(),
    );

    system
        .process(pid)
        .map(|proc| proc.memory() / 1024 / 1024)
        .expect("must get memory usage")
}
