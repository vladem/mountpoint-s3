use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use clap::{Arg, Command};
use futures::StreamExt;
use humansize::format_size;
use mountpoint_s3_client::config::{EndpointConfig, S3ClientConfig};
use mountpoint_s3_client::types::GetObjectParams;
use mountpoint_s3_client::{ObjectClient, S3CrtClient};
use mountpoint_s3_crt::common::rust_log_adapter::RustLogAdapter;
use regex::Regex;
use sysinfo::{get_current_pid, MemoryRefreshKind, ProcessRefreshKind, ProcessesToUpdate, System};
use tracing_subscriber::fmt::Subscriber;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use mountpoint_s3_client::part_pool::PartPool;

/// Like `tracing_subscriber::fmt::init` but sends logs to stderr
fn init_tracing_subscriber() {
    RustLogAdapter::try_init().expect("unable to install CRT log adapter");

    let subscriber = Subscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .finish();

    subscriber.try_init().expect("unable to install global subscriber");
}

fn configure_malloc() {
    unsafe {
        println!(
            "glibc version: {:?}",
            std::ffi::CStr::from_ptr(libc::gnu_get_libc_version())
        );
        libc::mallopt(libc::M_MMAP_THRESHOLD, 131072); // disable dynamic MMAP_THRESHOLD
    }
}

/// Creates requests sequentially and reads from each of them the specified number of bytes.
///
/// Example usage (read 8MiB from 50 requests):
/// cargo run --example unreleased_memory <BUCKET> <KEY> --range 0-8388608 --requests-num 50
fn main() {
    init_tracing_subscriber();

    let matches = Command::new("download")
        .about("Download a single key from S3")
        .arg(Arg::new("bucket").required(true))
        .arg(Arg::new("key").required(true))
        .arg(Arg::new("region").long("region").default_value("us-east-1"))
        .arg(
            Arg::new("requests")
                .long("requests-num")
                .default_value("1")
                .value_parser(clap::value_parser!(u64)),
        )
        .arg(
            Arg::new("tune-malloc")
                .long("tune-malloc")
                .default_value("false")
                .value_parser(clap::value_parser!(bool)),
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
    let requests = matches.get_one::<u64>("requests").unwrap();
    let tune_malloc = matches.get_one::<bool>("tune-malloc").unwrap();
    let range = matches.get_one::<String>("range").map(|s| {
        let range_regex = Regex::new(r"^(?P<start>[0-9]+)-(?P<end>[0-9]+)$").unwrap();
        let matches = range_regex.captures(s).expect("invalid range");
        let start = matches.name("start").unwrap().as_str().parse::<u64>().unwrap();
        let end = matches.name("end").unwrap().as_str().parse::<u64>().unwrap();
        // bytes range is inclusive, but the `Range` type is exclusive, so bump the end by 1
        start..(end + 1)
    });
    if *tune_malloc {
        configure_malloc();
    }

    let config = S3ClientConfig::new()
        .endpoint_config(EndpointConfig::new(region))
        .throughput_target_gbps(50.0);
    let part_pool = PartPool::new(config.read_part_size);
    let client = S3CrtClient::new(
        config,
        part_pool,
    )
    .expect("couldn't create client");

    let received_bytes_counter = Arc::new(AtomicU64::new(0));
    let received_bytes_counter_writer = received_bytes_counter.clone();
    std::thread::spawn(move || {
        let mut sys = System::new();
        loop {
            print_statistics(&mut sys, received_bytes_counter.clone());
            std::thread::sleep(std::time::Duration::from_secs(5));
        }
    });
    futures::executor::block_on(async move {
        // Create `requests` requests sequentially
        for _ in 0..*requests {
            let mut request = client
                .get_object(bucket, key, &GetObjectParams::new().range(range.clone()))
                .await
                .expect("couldn't create get request");
            // Read from each request `range` bytes before dropping it and creating a new one
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
        }
        // explicitly drop the client
        drop(client);
    });
    println!("dropped the client, sleeping");
    std::thread::sleep(std::time::Duration::from_secs(6));
}

fn print_statistics(sys: &mut System, received_bytes_counter: Arc<AtomicU64>) {
    println!(
        "read: {}",
        format_size(received_bytes_counter.load(Ordering::SeqCst), humansize::DECIMAL)
    );
    unsafe {
        let mallinfo = libc::mallinfo();
        println!("arena: {}", format_size(mallinfo.arena as u32, humansize::DECIMAL)); /* non-mmapped space allocated from system */
        println!("ordblks: {}", mallinfo.ordblks);
        println!("smblks: {}", mallinfo.smblks);
        println!("hblks: {}", mallinfo.hblks);
        println!("hblkhd: {}", format_size(mallinfo.hblkhd as u32, humansize::DECIMAL)); /* space in mmapped regions */
        println!("fsmblks: {}", format_size(mallinfo.fsmblks as u32, humansize::DECIMAL));
        println!(
            "uordblks: {}",
            format_size(mallinfo.uordblks as u32, humansize::DECIMAL)
        );
        println!(
            "fordblks: {}",
            format_size(mallinfo.fordblks as u32, humansize::DECIMAL)
        ); /* total free space */
        println!(
            "keepcost: {}",
            format_size(mallinfo.keepcost as u32, humansize::DECIMAL)
        );
    }
    if let Ok(pid) = get_current_pid() {
        sys.refresh_memory_specifics(MemoryRefreshKind::nothing().with_ram());
        sys.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            false,
            ProcessRefreshKind::nothing().with_memory(),
        );
        if let Some(process) = sys.process(pid) {
            println!("rss: {}", format_size(process.memory(), humansize::DECIMAL));
        }
    }
    println!("___");
}
