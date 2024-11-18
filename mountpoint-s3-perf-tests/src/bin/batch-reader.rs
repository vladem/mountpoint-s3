use clap::Parser;
use rayon::prelude::*;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::Path;
use std::time::{Duration, Instant};
use crossbeam::channel::{self, Sender};
use std::thread;

#[derive(Parser)]
#[command(name = "discard_files")]
#[command(version = "1.0")]
struct Args {
    /// The directory to process
    #[arg(value_name = "DIRECTORY")]
    directory: String,

    /// The number of files to process in parallel at once
    #[arg(short, long, default_value_t = 8)]
    worker_num: usize,
}

fn read_and_discard_file(file_path: &Path) -> io::Result<()> {
    let mut file = File::open(file_path)?;
    let mut buffer = Vec::new();

    // Read the file into a buffer (content is discarded)
    file.read_to_end(&mut buffer)?;

    Ok(())
}

fn read_and_discard_files(directory: &str, worker_num: usize, results: Sender<Option<Duration>>) -> io::Result<()> {
    // List files in the specified directory
    let entries = fs::read_dir(directory)?;

    let mut files = Vec::new();

    // Collect only files (not directories)
    for entry in entries {
        let entry = entry?;
        if entry.path().is_file() {
            files.push(entry.path());
        }
    }

    if files.is_empty() {
        println!("No files found in directory: {}", directory);
        return Ok(());
    }

    println!(
        "Found {} file(s) in the directory '{}', processing by {} threads.",
        files.len(),
        directory,
        worker_num
    );

    files
        .into_par_iter()
        .for_each(|file_path| {
            let start_time = Instant::now();
            if let Err(e) = read_and_discard_file(&file_path) {
                eprintln!("Error processing file {}: {}", file_path.display(), e);
                return;
            }
            results.send(Some(start_time.elapsed())).unwrap();
        });
    Ok(())
}

fn main() {
    // Parse the arguments using clap's derive syntax
    let args = Args::parse();

    // Configure the thread pool
    rayon::ThreadPoolBuilder::new()
        .num_threads(args.worker_num)
        .build_global()
        .unwrap();

    // Create an unbounded channel to process results
    let (tx, rx) = channel::unbounded::<Option<Duration>>();

    // Spawn a thread to print items from the queue
    let printer_thread = thread::spawn(move || {
        loop {
            let value = rx.recv().unwrap();
            let Some(value) = value else {
                break;
            };
            println!("{}", value.as_millis());
        }
    });

    // Call the function to process the files in the given directory
    let start_time = Instant::now();
    if let Err(e) = read_and_discard_files(&args.directory, args.worker_num, tx.clone()) {
        eprintln!("Error: {}", e);
    }

    // Send the overall time and wait for all values to be printed
    tx.send(Some(start_time.elapsed())).unwrap();
    tx.send(None).unwrap();
    printer_thread.join().unwrap();
}
