use clap::Parser;
use mountpoint_s3_fs::manifest::{create_db, CsvReader, ManifestError};
use std::{path::PathBuf, time::Instant};

#[derive(Parser, Debug)]
#[clap(name = "manifest-to-sqlite", about = "Helper for Mountpoint for Amazon S3")]
struct CliArgs {
    #[clap(
        help = "A CSV file of triplets <base64(name),etag,size> representing keys in the bucket",
        value_name = "INPUT_CSV"
    )]
    input_csv: PathBuf,

    #[clap(
        help = "Path to the output database file (will be created)",
        value_name = "OUTPUT_FILE"
    )]
    db_path: PathBuf,
}

fn main() -> Result<(), ManifestError> {
    let args = CliArgs::parse();

    let batch_size = 100000usize;
    let start = Instant::now();
    create_db(&args.db_path, CsvReader::new(&args.input_csv)?, batch_size)?;
    println!("creation took: {:?}", start.elapsed());

    Ok(())
}
