use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use clap::{Arg, Command};
use futures::task::SpawnExt;
use futures::StreamExt;
use humansize::format_size;
use mountpoint_s3_client::config::{EndpointConfig, S3ClientConfig};
use mountpoint_s3_client::types::GetObjectParams;
use mountpoint_s3_client::{ObjectClient, S3CrtClient};
use mountpoint_s3_crt::common::rust_log_adapter::RustLogAdapter;
use regex::Regex;
use std::time::Instant;
use sysinfo::{get_current_pid, MemoryRefreshKind, ProcessRefreshKind, ProcessesToUpdate, System};
use tracing_subscriber::fmt::Subscriber;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Like `tracing_subscriber::fmt::init` but sends logs to stderr
fn init_tracing_subscriber() {
    RustLogAdapter::try_init().expect("unable to install CRT log adapter");

    let subscriber = Subscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .finish();

    subscriber.try_init().expect("unable to install global subscriber");
}

/// Splits the given range in `requests-num` non-overlapping sub-ranges and spawns requests in parallel
/// for each sub-range.
///
/// Example usage (read 1GiB from 128 requests, 8MiB each):
/// cargo run --example unreleased_memory <BUCKET> <KEY> --range 0-1073741824 --requests-num 128
fn main() {
    init_tracing_subscriber();

    let matches = Command::new("download")
        .about("Download a single key from S3")
        .arg(Arg::new("bucket").required(true))
        .arg(Arg::new("key").required(true))
        .arg(Arg::new("region").long("region").default_value("us-east-1"))
        .arg(
            Arg::new("requests-num")
                .long("requests-num")
                .default_value("1")
                .value_parser(clap::value_parser!(u64)),
        )
        .arg(
            Arg::new("part-size")
                .long("part-size")
                .default_value("8388608")
                .value_parser(clap::value_parser!(u64)),
        )
        .arg(
            Arg::new("range")
                .long("range")
                .help("byte range to download (inclusive)")
                .value_name("0-10"),
        )
        .get_matches();

    let bucket = matches.get_one::<String>("bucket").unwrap();
    let key = matches.get_one::<String>("key").unwrap();
    let region = matches.get_one::<String>("region").unwrap();
    let requests_num = matches.get_one::<u64>("requests-num").unwrap();
    let part_size = matches.get_one::<u64>("part-size").unwrap();
    let part_size = *part_size as usize;
    let range = matches
        .get_one::<String>("range")
        .map(|s| {
            let range_regex = Regex::new(r"^(?P<start>[0-9]+)-(?P<end>[0-9]+)$").unwrap();
            let matches = range_regex.captures(s).expect("invalid range");
            let start = matches.name("start").unwrap().as_str().parse::<u64>().unwrap();
            let end = matches.name("end").unwrap().as_str().parse::<u64>().unwrap();
            // We treat the provided range as half-open: [start, end)
            start..end
        })
        .expect("range is required");

    let config = S3ClientConfig::new()
        .endpoint_config(EndpointConfig::new(region))
        .part_size(part_size);

    let part_pool = mountpoint_s3_client::part_pool::PartPool::new(part_size);
    println!("Using part pool");
    let client = S3CrtClient::new(config, part_pool).expect("couldn't create client");

    // println!("Not using part pool");
    // let client = S3CrtClient::new(config).expect("couldn't create client");

    let start = Instant::now();
    let received_bytes_counter = Arc::new(AtomicU64::new(0));
    let received_bytes_counter_reader = received_bytes_counter.clone();
    let received_bytes_counter_writer = received_bytes_counter.clone();
    std::thread::spawn(move || {
        let mut sys = System::new();
        loop {
            print_statistics(&mut sys, received_bytes_counter_reader.clone());
            std::thread::sleep(std::time::Duration::from_secs(10));
        }
    });
    futures::executor::block_on(async move {
        // Split the range to `requests` requests and execute them concurrently
        let mut requests = vec![];
        let request_ranges = get_request_ranges(range, *requests_num);
        println!("request_ranges: {:?}", request_ranges);
        for request_range in request_ranges {
            let bucket = bucket.to_owned();
            let key = key.to_owned();
            let client = client.clone();
            let received_bytes_counter_writer = received_bytes_counter_writer.clone();
            let task = client.event_loop_group().spawn_with_handle(async move {
                let mut request = client
                    .get_object(&bucket, &key, &GetObjectParams::new().range(Some(request_range)))
                    .await
                    .expect("couldn't create get request");
                // Read from each request till the end
                loop {
                    match StreamExt::next(&mut request).await {
                        Some(Ok((_, body))) => {
                            received_bytes_counter_writer.fetch_add(body.len() as u64, Ordering::SeqCst);
                        }
                        Some(Err(e)) => {
                            tracing::error!(error = ?e, "request failed");
                            break;
                        }
                        None => break,
                    }
                }
            });
            requests.push(task.expect("spawn should succeed"));
        }

        // wait for all requests to complete
        for handle in requests {
            handle.await;
        }

        // explicitly drop the client
        drop(client);
    });

    println!(
        "bandwidth: {} MiB/s",
        received_bytes_counter.load(Ordering::SeqCst) as f64 / start.elapsed().as_secs_f64() / 1024.0 / 1024.0
    );

    // It still takes sometime to observe the RSS decrease
    std::thread::sleep(std::time::Duration::from_secs(1));

    let mut sys = System::new();
    print_statistics(&mut sys, received_bytes_counter);
}

fn print_statistics(sys: &mut System, received_bytes_counter: Arc<AtomicU64>) {
    println!(
        "read: {}",
        format_size(received_bytes_counter.load(Ordering::SeqCst), humansize::BINARY)
    );
    unsafe {
        let mallinfo = libc::mallinfo();
        println!("arena: {}", format_size(mallinfo.arena as u32, humansize::BINARY)); /* non-mmapped space allocated from system */
        println!("hblkhd: {}", format_size(mallinfo.hblkhd as u32, humansize::BINARY)); /* space in mmapped regions */
        println!("fordblks: {}", format_size(mallinfo.fordblks as u32, humansize::BINARY)); /* total free space */
        println!("keepcost: {}", format_size(mallinfo.keepcost as u32, humansize::BINARY));
    }
    if let Ok(pid) = get_current_pid() {
        sys.refresh_memory_specifics(MemoryRefreshKind::nothing().with_ram());
        sys.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            false,
            ProcessRefreshKind::nothing().with_memory(),
        );
        if let Some(process) = sys.process(pid) {
            println!("rss: {}", format_size(process.memory(), humansize::BINARY));
        }
    }
    println!("___");
}

fn get_request_ranges(download_range: Range<u64>, requests_num: u64) -> Vec<Range<u64>> {
    let total_bytes = download_range.end - download_range.start;
    let step = total_bytes / requests_num;
    let download_range_end = download_range.end;
    download_range
        .step_by(step as usize)
        .map(|start| (start..(start + step).min(download_range_end)))
        .collect()
}
