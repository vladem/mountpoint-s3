#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> anyhow::Result<()> {
    mountpoint_s3::cli::main(mountpoint_s3::cli::create_s3_client)
}
