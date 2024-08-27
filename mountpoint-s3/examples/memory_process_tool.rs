use anyhow::Result;
use clap::Parser;
use crossbeam_channel::{bounded, select, tick, Receiver};
use humansize::make_format;
use std::{
    fs::{self, File},
    io::{BufReader, Read},
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
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
    #[clap(long, help = "Timeout in seconds [default: 300]", default_value = "300")]
    timeout: u32,
    #[clap(long, help = "Print results header (useful for first result in a file)")]
    print_header: bool,
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
        "Starting monitoring tool with mountpoint_pid={} start_read={} start_copy={} num_tasks={} print_header={}",
        args.mountpoint_pid, args.start_read, args.start_copy, args.num_tasks, args.print_header
    );

    let completed = Arc::new(AtomicBool::new(false));

    let ctrl_c_events = ctrl_channel()?;
    let ticks = tick(Duration::from_secs(1));

    let start_time = Instant::now();
    let mut thread_handles: Vec<thread::JoinHandle<()>> = Vec::new();
    let total_read_bytes = Arc::new(AtomicU64::new(0));
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
            let total_read_bytes_clone = total_read_bytes.clone();
            let handle = thread::spawn(move || {
                let src_dir = Path::new("/home/ubuntu/mounted-s3");
                let src = src_dir.join(format!("sequential_read_direct_io.0.0"));
                const READ_SIZE: usize = 4 * 1024;
                let mut buffer = [0; READ_SIZE];

                while start_time.elapsed().as_secs() < args.timeout.into() {
                    println!("thread {} start reading file {}", i, src.to_string_lossy());
                    let start = Instant::now();
                    let f = File::open(src.clone()).unwrap();
                    let mut f = BufReader::new(f);
                    while let Ok(n) = f.read(&mut buffer[..]) {
                        total_read_bytes_clone.fetch_add(n as u64, Ordering::SeqCst);
                        if n == 0 || start.elapsed().as_secs() > args.timeout.into() {
                            break;
                        }
                    }
                }
            });
            thread_handles.push(handle);
        }
    }

    let mut sys = System::new_all();
    let mut total_samples: u64 = 0;
    let mut mem_usage_sum: u64 = 0;
    let mut max_mem_usage: u64 = 0;
    let mut samples_above_target: u64 = 0;
    let mut samples_above_2x_target: u64 = 0;
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
                    mem_usage_sum += mem_usage;
                    if let Some(max_mem_target) = args.max_memory_target {
                        if mem_usage > max_mem_target * 1024 * 1024 {
                            samples_above_target += 1;
                        }
                        if mem_usage > max_mem_target * 1024 * 1024 * 2{
                            samples_above_2x_target += 1;
                        }
                    }
                    if mem_usage > max_mem_usage {
                        max_mem_usage = mem_usage;
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
                    break;
                }
            }
            recv(ctrl_c_events) -> _ => {
                break;
            }
        }
    }

    #[derive(Default)]
    struct Stat {
        target_mib: u64,
        threads: u64,
        avg_throughput_mibs: u64,
        max_rss_mib: u64,
        avg_rss_mib: u64,
        ge_target: f64,
        ge_2x_target: f64,
        elapsed_s: u64,
    };

    let elapsed = start_time.elapsed().as_secs();
    let total_read_bytes = total_read_bytes.load(Ordering::SeqCst);

    let mut stat = Stat{
        threads: args.num_tasks as u64,
        elapsed_s: elapsed,
        avg_throughput_mibs: (total_read_bytes as f64 / elapsed as f64 / 1024.0 / 1024.0) as u64,
        max_rss_mib: (max_mem_usage / 1024 / 1024) as u64,
        avg_rss_mib: mem_usage_sum / 1024 / 1024 / total_samples,
        ..Default::default()
    };
    if let Some(max_mem_target) = args.max_memory_target {
        stat.target_mib = max_mem_target;
        stat.ge_target = samples_above_target as f64 / total_samples as f64;
        stat.ge_2x_target = samples_above_2x_target as f64 / total_samples as f64;
    };

    println!(
        "target_mib:{}\nthreads:{}\navg_throughput_mibs:{}\nmax_rss_mib:{}\navg_rss_mib:{}\nge_target:{}\nge_2x_target:{}\nelapsed_s:{}\n",
        stat.target_mib,
        stat.threads,
        stat.avg_throughput_mibs,
        stat.max_rss_mib,
        stat.avg_rss_mib,
        stat.ge_target,
        stat.ge_2x_target,
        stat.elapsed_s,
    );
    if args.print_header {
        eprintln!("target_mib,threads,avg_throughput_mibs,max_rss_mib,avg_rss_mib,ge_target,ge_2x_target,elapsed_s");
    }
    eprintln!(
        "{},{},{},{},{},{},{},{}",
        stat.target_mib,
        stat.threads,
        stat.avg_throughput_mibs,
        stat.max_rss_mib,
        stat.avg_rss_mib,
        stat.ge_target,
        stat.ge_2x_target,
        stat.elapsed_s,
    );
    Ok(())
}
