use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

use clap::Parser;
use futures::future::join_all;

use mountpoint_s3_client::config::{EndpointConfig, S3ClientConfig};
use mountpoint_s3_client::types::PutObjectParams;
use mountpoint_s3_client::{ObjectClient, PutObjectRequest, S3CrtClient};

#[derive(Parser)]
#[command(about = "Upload N GiB of zero bytes to S3 and measure bandwidth")]
struct Args {
    /// S3 bucket name
    #[arg(long)]
    bucket: String,

    /// AWS region
    #[arg(long)]
    region: String,

    /// Destination S3 key
    #[arg(long)]
    key: String,

    /// Size in GiB for each thread to upload
    #[arg(long)]
    size: u64,

    #[arg(long)]
    part_size: u64,

    /// Number of parallel upload threads
    #[arg(long, default_value = "1")]
    threads: usize,
}

async fn upload_data(
    client: Arc<S3CrtClient>,
    bucket: String,
    key: String,
    total_size: u64,
) -> Result<f64, Box<dyn Error + Send + Sync>> {
    // Initialize upload request
    let mut request = client.put_object(&bucket, &key, &PutObjectParams::new()).await?;

    // Create a 128 MiB zero buffer for streaming
    let buffer = vec![0u8; 128 * 1024 * 1024];
    let mut remaining = total_size;

    let start = Instant::now();

    // Stream the data in 128 MiB chunks
    while remaining > 0 {
        let chunk_size = std::cmp::min(remaining, buffer.len() as u64) as usize;
        request.write(&buffer[..chunk_size]).await?;
        remaining -= chunk_size as u64;
    }

    // Complete the upload
    request.complete().await?;

    // Calculate and return bandwidth in MiB/s
    let elapsed = start.elapsed();
    Ok((total_size as f64) / elapsed.as_secs_f64() / (1024.0 * 1024.0))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    // Convert GiB to bytes (per thread)
    let size_per_thread = args.size * 1024 * 1024 * 1024;
    let total_size = size_per_thread * args.threads as u64;

    // Create client
    let config = S3ClientConfig::new()
        .endpoint_config(EndpointConfig::new(&args.region))
        .part_size(args.part_size as usize)
        .throughput_target_gbps(100.0);

    let client = Arc::new(S3CrtClient::new(config)?);
    println!(
        "Starting {} parallel uploads of {} GiB each to s3://{}/{} (total {} GiB)",
        args.threads,
        args.size,
        args.bucket,
        args.key,
        args.size * args.threads as u64,
    );

    let start = Instant::now();

    let mut tasks = Vec::new();
    for i in 0..args.threads {
        let client = Arc::clone(&client);
        let bucket = args.bucket.clone();
        let key = format!("{}.part{}", args.key, i);
        tasks.push(upload_data(client, bucket, key, size_per_thread));
    }

    let thread_bandwidths: Result<Vec<f64>, _> = join_all(tasks).await.into_iter().collect();
    let thread_bandwidths = thread_bandwidths.expect("therads must succeed");
    let elapsed = start.elapsed();
    let total_bandwidth = (total_size as f64) / elapsed.as_secs_f64() / (1024.0 * 1024.0);
    let avg_thread_bandwidth: f64 = thread_bandwidths.iter().sum::<f64>() / args.threads as f64;

    println!("All uploads complete!");
    println!("Total time: {:.2} seconds", elapsed.as_secs_f64());
    println!("Total bandwidth: {:.2} MiB/s", total_bandwidth);
    println!("Average bandwidth per thread: {:.2} MiB/s", avg_thread_bandwidth);

    Ok(())
}
