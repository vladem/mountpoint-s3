# Usage: nohup python3 file_reader_spawner.py 20 &
# kill -TERM <parent_pid>

import os
import sys
import time
import signal
import multiprocessing
import fcntl

# Constants
FILE_DIR = "/home/vlaad/mounted-s3"

# Graceful shutdown flag
terminate_flag = multiprocessing.Event()

def handle_termination(signum, frame):
    print(f"[Parent] Caught signal {signum}, terminating subprocesses...")
    terminate_flag.set()

def file_reader(proc_id: int, total_files: int):
    file_index = proc_id % total_files + 1
    file_path = os.path.join(FILE_DIR, f"file_{file_index}.dat")

    print(f"[Process-{proc_id}] Assigned to file: {file_path}")

    try:
        # Open file with O_DIRECT to bypass kernel cache
        fd = os.open(file_path, os.O_RDONLY | os.O_DIRECT)
        f = os.fdopen(fd, 'rb')

        try:
            while not terminate_flag.is_set():
                read_bytes = 0
                print(f"[Process-{proc_id}] Reading file: {file_path} from the start")
                try:
                    while True:
                        chunk = f.read(1024 * 1024)  # Read in 1MB chunks
                        read_bytes += len(chunk)
                        if not chunk:
                            # EOF reached, seek back to beginning instead of re-opening
                            f.seek(0)
                            print(f"[Process-{proc_id}] Read {read_bytes} bytes")
                            break
                except Exception as e:
                    print(f"[Process-{proc_id}] Warning: {e}")
                    time.sleep(5)
                    # Seek to beginning after error to continue reading
                    f.seek(0)
                    continue
        finally:
            f.close()
    except Exception as e:
        print(f"[Process-{proc_id}] Fatal error opening file: {e}")

    print(f"[Process-{proc_id}] Terminating.")

def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <num_processes>")
        sys.exit(1)

    try:
        num_processes = int(sys.argv[1])
    except ValueError:
        print("Error: num_processes must be an integer.")
        sys.exit(1)

    # Count how many files exist in the directory
    available_files = [
        f for f in os.listdir(FILE_DIR) if f.startswith("file_") and f.endswith(".dat")
    ]
    total_files = len(available_files)

    if total_files == 0:
        print(f"Error: No files found in {FILE_DIR}")
        sys.exit(1)

    print(f"[Parent] Spawning {num_processes} subprocesses with {total_files} files.")

    # Set signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, handle_termination)
    signal.signal(signal.SIGTERM, handle_termination)

    # Start subprocesses
    processes = []
    for i in range(num_processes):
        p = multiprocessing.Process(target=file_reader, args=(i, total_files))
        p.start()
        processes.append(p)

    # Wait for termination signal
    try:
        while not terminate_flag.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        handle_termination(signal.SIGINT, None)

    # Wait for all children to exit
    for p in processes:
        p.terminate()
        p.join()

    print("[Parent] All subprocesses terminated.")

if __name__ == "__main__":
    main()
