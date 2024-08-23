use anyhow::Result;
use clap::Parser;
use crossbeam_channel::{bounded, select, tick, Receiver};
use humansize::make_format;
use std::{
    fs::{self, File},
    io::{BufReader, Read},
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant, UNIX_EPOCH, SystemTime},
};
use sysinfo::{Pid, System};

#[derive(Parser, Debug)]
struct MainCli {
    /// Mountpoint process id to monitor
    mountpoint_pid: u32,
    /// Start copy tasks
    #[clap(long, help = "Set to start copy tasks")]
    start_copy: bool,
    /// Start the read tasks
    #[clap(long, help = "Set to start read tasks")]
    start_read: bool,
    /// Copy task number
    #[clap(long, help = "Number of copy tasks [default: 1]", default_value = "1")]
    num_tasks: u32,
    #[clap(long, help = "Maximum memory usage target")]
    pub max_memory_target: Option<u64>,
}

fn ctrl_channel() -> Result<Receiver<()>, ctrlc::Error> {
    let (sender, receiver) = bounded(100);
    ctrlc::set_handler(move || {
        let _ = sender.send(());
    })?;

    Ok(receiver)
}

fn main() -> Result<()> {
    let args = MainCli::parse();
    println!(
        "Starting monitoring tool with mountpoint_pid={} start_read={} start_copy={} num_tasks={}",
        args.mountpoint_pid, args.start_read, args.start_copy, args.num_tasks,
    );

    let completed = Arc::new(AtomicBool::new(false));

    let ctrl_c_events = ctrl_channel()?;
    let ticks = tick(Duration::from_secs(1));

    let start_time = Instant::now();
    let mut thread_handles: Vec<thread::JoinHandle<()>> = Vec::new();
    if args.start_copy {
        for i in 0..args.num_tasks {
            let handle = thread::spawn(move || {
                let src_dir = Path::new("/home/ubuntu/mounted-s3");
                let dest = Path::new("/dev/null");
                let src = src_dir.join(format!("sequential_read_direct_io.0.0"));
                println!(
                    "start copying file from {} to {}",
                    src.to_string_lossy(),
                    dest.to_string_lossy(),
                );
                fs::copy(&src, &dest).unwrap();
                println!("copying to file {} is complete", dest.to_string_lossy());
            });
            thread_handles.push(handle);
        }
    }

    if args.start_read {
        for i in 0..args.num_tasks {
            let handle = thread::spawn(move || {
                let src_dir = Path::new("/home/ubuntu/mounted-s3");
                let src = src_dir.join(format!("sequential_read_direct_io.0.0"));
                const READ_SIZE: usize = 4 * 1024;
                let mut buffer = [0; READ_SIZE];

                loop {
                    println!("thread {} start reading file {}", i, src.to_string_lossy());
                    let start = Instant::now();
                    let f = File::open(src.clone()).unwrap();
                    let mut f = BufReader::new(f);
                    while let Ok(n) = f.read(&mut buffer[..]) {
                        // if n == 0 || start.elapsed().as_secs() > (30 + i * 2).into() {
                        if n == 0 {
                            break;
                        }
                    }
                }
            });
            thread_handles.push(handle);
        }
    }

    let mut sys = System::new_all();
    let mut total_samples = 0;
    let mut samples_above_target = 0;
    loop {
        select! {
            recv(ticks) -> _ => {
                let pid = Pid::from(args.mountpoint_pid as usize);
                sys.refresh_process(pid);
                if let Some(process) = sys.process(pid) {
                    let formatter = make_format(humansize::BINARY);
                    let mem_usage = process.memory();
                    println!("{},{}", mem_usage, formatter(process.memory()));
                    total_samples += 1;
                    if let Some(max_mem_target) = args.max_memory_target {
                        if mem_usage > ((max_mem_target * 1024 * 1024) as f64 * 1.5) as u64 {
                            samples_above_target += 1;
                        }
                    }
                } else {
                    println!("pid not found!");
                }

                let mut all_finished = true;
                for handle in thread_handles.iter() {
                    if !handle.is_finished() {
                        all_finished = false;
                        break;
                    }
                }
                if all_finished {
                    let elapsed = start_time.elapsed();
                    println!("Exiting program, elapsed time {} secs", elapsed.as_secs());
                    break;
                }
            }
            recv(ctrl_c_events) -> _ => {
                break;
            }
        }
    }

    if let Some(max_mem_target) = args.max_memory_target {
        println!("samples above target: {}", samples_above_target as f64 / total_samples as f64)
    }

    Ok(())
}
