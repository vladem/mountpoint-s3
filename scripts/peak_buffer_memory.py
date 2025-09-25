#!/usr/bin/env python3
"""
Script to analyze buffer allocations from mountpoint-s3 logs and find peak memory usage.
Based on trace_allocations.py but focuses on finding the timestamp with maximum total memory usage.
"""

import re
import sys
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from collections import defaultdict


class Colors:
    """ANSI color codes for terminal output."""
    RESET = '\033[0m'
    BOLD = '\033[1m'
    DIM = '\033[2m'

    # Colors
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'

    # Background colors
    BG_RED = '\033[101m'
    BG_GREEN = '\033[102m'
    BG_YELLOW = '\033[103m'
    BG_BLUE = '\033[104m'

    @staticmethod
    def disable():
        """Disable colors (for non-terminal output)."""
        Colors.RESET = Colors.BOLD = Colors.DIM = ''
        Colors.RED = Colors.GREEN = Colors.YELLOW = Colors.BLUE = ''
        Colors.MAGENTA = Colors.CYAN = Colors.WHITE = ''
        Colors.BG_RED = Colors.BG_GREEN = Colors.BG_YELLOW = Colors.BG_BLUE = ''


def colored(text: str, color: str, bold: bool = False) -> str:
    """Return colored text."""
    prefix = Colors.BOLD if bold else ''
    return f"{prefix}{color}{text}{Colors.RESET}"


def format_bytes(bytes_val: int) -> str:
    """Format bytes with appropriate unit and color."""
    if bytes_val >= 1024 * 1024 * 1024:  # GB
        return colored(f"{bytes_val/1024/1024/1024:.2f} GB", Colors.RED, bold=True)
    elif bytes_val >= 1024 * 1024:  # MB
        return colored(f"{bytes_val/1024/1024:.2f} MB", Colors.YELLOW, bold=True)
    elif bytes_val >= 1024:  # KB
        return colored(f"{bytes_val/1024:.2f} KB", Colors.GREEN, bold=True)
    else:
        return colored(f"{bytes_val} bytes", Colors.CYAN, bold=True)


@dataclass
class BufferEvent:
    """Represents a buffer allocation or deallocation event."""
    timestamp: datetime
    event_type: str  # 'allocated' or 'deallocated'
    addr: str
    s3_key: str
    offset: int
    payload_size: int


@dataclass
class BackpressureEvent:
    """Represents a backpressure feedback event."""
    timestamp: datetime
    event_type: str  # 'DataRead' or 'IncrementReadWindow'
    name: str  # object name from name="file_1.dat"
    offset: Optional[int] = None
    length: Optional[int] = None
    new_read_window_end_offset: Optional[int] = None


class PeakMemoryAnalyzer:
    """Analyzes buffer allocations to find peak memory usage."""

    def __init__(self):
        self.events: List[BufferEvent] = []
        self.backpressure_events: List[BackpressureEvent] = []

    def parse_log_line(self, line: str) -> Optional[BufferEvent]:
        """Parse a single log line and extract buffer event if present."""
        # Match buffer allocation/deallocation events
        buffer_pattern = r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z).*buffer_(allocated|deallocated)\s+addr=(\d+)\s+s3_key=([^\s]+)\s+offset=(\d+)\s+payload_size=(\d+)'

        match = re.search(buffer_pattern, line)
        if not match:
            return None

        timestamp_str, event_type, addr, s3_key, offset, payload_size = match.groups()

        # Parse timestamp
        timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))

        return BufferEvent(
            timestamp=timestamp,
            event_type=event_type,
            addr=addr,
            s3_key=s3_key,
            offset=int(offset),
            payload_size=int(payload_size)
        )

    def parse_backpressure_line(self, line: str) -> Optional[BackpressureEvent]:
        """Parse a single log line and extract backpressure event if present."""
        # Match BackpressureFeedbackEvent::DataRead with key field
        data_read_pattern = r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z).*BackpressureFeedbackEvent::DataRead\s+key="([^"]+)"\s+offset=(\d+)\s+length=(\d+)'

        # Match BackpressureFeedbackEvent::IncrementReadWindow with key field and read_window_end_offset
        increment_window_pattern = r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z).*BackpressureFeedbackEvent::IncrementReadWindow\s+key="([^"]+)"\s+new_read_window_end_offset=(\d+)'

        # Try DataRead pattern first
        match = re.search(data_read_pattern, line)
        if match:
            timestamp_str, key, offset, length = match.groups()
            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return BackpressureEvent(
                timestamp=timestamp,
                event_type='DataRead',
                name=key,
                offset=int(offset),
                length=int(length)
            )

        # Try IncrementReadWindow pattern
        match = re.search(increment_window_pattern, line)
        if match:
            timestamp_str, key, read_window_end_offset = match.groups()
            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return BackpressureEvent(
                timestamp=timestamp,
                event_type='IncrementReadWindow',
                name=key,
                new_read_window_end_offset=int(read_window_end_offset)
            )

        return None

    def parse_log_file(self, filename: str):
        """Parse an entire log file and extract all buffer and backpressure events."""
        try:
            with open(filename, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        # Try to parse buffer event
                        buffer_event = self.parse_log_line(line)
                        if buffer_event:
                            self.events.append(buffer_event)

                        # Try to parse backpressure event
                        backpressure_event = self.parse_backpressure_line(line)
                        if backpressure_event:
                            self.backpressure_events.append(backpressure_event)
                    except Exception as e:
                        print(f"Warning: Error parsing line {line_num}: {e}", file=sys.stderr)
        except FileNotFoundError:
            print(f"Error: Log file '{filename}' not found", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"Error reading log file: {e}", file=sys.stderr)
            sys.exit(1)

    def calculate_memory_usage_over_time(self) -> List[Tuple[datetime, int]]:
        """
        Calculate total memory usage at each event timestamp.
        Returns list of (timestamp, total_memory_bytes) tuples.
        """
        if not self.events:
            return []

        # Sort events by timestamp
        sorted_events = sorted(self.events, key=lambda e: e.timestamp)

        # Track active allocations
        active_allocations = defaultdict(dict)  # s3_key -> {addr -> event}
        memory_timeline = []

        for event in sorted_events:
            if event.event_type == 'allocated':
                active_allocations[event.s3_key][event.addr] = event
            elif event.event_type == 'deallocated':
                if event.s3_key in active_allocations and event.addr in active_allocations[event.s3_key]:
                    del active_allocations[event.s3_key][event.addr]
                    # Clean up empty s3_key entries
                    if not active_allocations[event.s3_key]:
                        del active_allocations[event.s3_key]

            # Calculate total memory usage at this timestamp
            total_memory = 0
            for s3_key_allocations in active_allocations.values():
                for _ in s3_key_allocations.values():
                    # total_memory += allocation_event.payload_size # TODO: this looks like the source of error (low values), we meed to count actual buffer sizes
                    total_memory += 8 * 1024**2

            memory_timeline.append((event.timestamp, total_memory))

        return memory_timeline

    def find_peak_memory_usage(self) -> Tuple[datetime, int, Dict[str, List[Tuple[int, int]]]]:
        """
        Find the timestamp with peak memory usage and return allocations at that time.
        Returns (peak_timestamp, peak_memory_bytes, allocations_dict).
        """
        memory_timeline = self.calculate_memory_usage_over_time()

        if not memory_timeline:
            return None, 0, {}

        # Find peak memory usage
        peak_timestamp, peak_memory = max(memory_timeline, key=lambda x: x[1])

        # Get allocations at peak timestamp
        allocations = self.get_allocations_at_timestamp(peak_timestamp)

        return peak_timestamp, peak_memory, allocations

    def get_allocations_at_timestamp(self, target_timestamp: datetime) -> Dict[str, List[Tuple[int, int]]]:
        """
        Get all buffer allocations that were active at a given timestamp.
        Returns a dict mapping s3_key to list of (offset, payload_size) tuples.
        """
        # Reset allocations and replay events up to the target timestamp
        temp_allocations = defaultdict(dict)

        for event in sorted(self.events, key=lambda e: e.timestamp):
            if event.timestamp > target_timestamp:
                break

            if event.event_type == 'allocated':
                temp_allocations[event.s3_key][event.addr] = event
            elif event.event_type == 'deallocated':
                if event.s3_key in temp_allocations and event.addr in temp_allocations[event.s3_key]:
                    del temp_allocations[event.s3_key][event.addr]

        # Convert to the desired format
        result = {}
        for s3_key, addr_map in temp_allocations.items():
            result[s3_key] = [(event.offset, event.payload_size) for event in addr_map.values()]
            # Sort by offset for easier reading
            result[s3_key].sort(key=lambda x: x[0])

        return result

    def get_read_window_at_timestamp(self, target_timestamp: datetime) -> Dict[str, Tuple[Optional[int], Optional[int], Optional[int]]]:
        """
        Calculate read window start and end offsets at a given timestamp, grouped by object name.
        Returns dict mapping object name to (start_offset, end_offset, size) tuple.

        Window start = offset + length from last observed DataRead event
        Window end = new_read_window_end_offset from last observed IncrementReadWindow event
        """
        if not self.backpressure_events:
            return {}

        # Sort backpressure events by timestamp
        sorted_events = sorted(self.backpressure_events, key=lambda e: e.timestamp)

        # Track last events per object name
        last_data_read_per_name = {}
        last_increment_window_per_name = {}

        # Find the last DataRead and IncrementReadWindow events before target timestamp for each object
        for event in sorted_events:
            if event.timestamp > target_timestamp:
                break

            if event.event_type == 'DataRead':
                last_data_read_per_name[event.name] = event
            elif event.event_type == 'IncrementReadWindow':
                last_increment_window_per_name[event.name] = event

        # Calculate window boundaries for each object
        result = {}
        all_object_names = set(last_data_read_per_name.keys()) | set(last_increment_window_per_name.keys())

        for name in all_object_names:
            start_offset = None
            end_offset = None

            # Get start offset from DataRead event
            if name in last_data_read_per_name:
                data_read_event = last_data_read_per_name[name]
                if data_read_event.offset is not None and data_read_event.length is not None:
                    start_offset = data_read_event.offset + data_read_event.length

            # Get end offset from IncrementReadWindow event
            if name in last_increment_window_per_name:
                increment_event = last_increment_window_per_name[name]
                if increment_event.new_read_window_end_offset is not None:
                    end_offset = increment_event.new_read_window_end_offset

            # Calculate size if both boundaries are available
            size = 0
            if start_offset is not None and end_offset is not None and end_offset > start_offset:
                size = end_offset - start_offset

            result[name] = (start_offset, end_offset, size)

        return result

    def print_summary(self):
        """Print a summary of all events."""
        print(colored("📊 BUFFER ALLOCATION SUMMARY", Colors.CYAN, bold=True))
        print("=" * 60)

        total_events = len(self.events)
        allocated_count = sum(1 for e in self.events if e.event_type == 'allocated')
        deallocated_count = sum(1 for e in self.events if e.event_type == 'deallocated')

        print(f"Total events parsed: {colored(str(total_events), Colors.WHITE, bold=True)}")
        print(f"📈 Allocations: {colored(str(allocated_count), Colors.GREEN, bold=True)}")
        print(f"📉 Deallocations: {colored(str(deallocated_count), Colors.RED, bold=True)}")

        if allocated_count != deallocated_count:
            diff = allocated_count - deallocated_count
            if diff > 0:
                print(f"⚠️  Net allocations: {colored(f'+{diff}', Colors.YELLOW, bold=True)} (potential memory leaks)")
            else:
                print(f"✅ Net allocations: {colored(str(diff), Colors.GREEN, bold=True)}")

        if self.events:
            first_event = min(self.events, key=lambda e: e.timestamp)
            last_event = max(self.events, key=lambda e: e.timestamp)
            duration = last_event.timestamp - first_event.timestamp
            print(f"⏰ Time range: {colored(str(first_event.timestamp), Colors.BLUE)} to {colored(str(last_event.timestamp), Colors.BLUE)}")
            print(f"⏱️  Duration: {colored(str(duration), Colors.MAGENTA, bold=True)}")

        unique_keys = len(set(e.s3_key for e in self.events))
        print(f"🗂️  Unique S3 keys: {colored(str(unique_keys), Colors.CYAN, bold=True)}")

        total_backpressure_events = len(self.backpressure_events)
        data_read_count = sum(1 for e in self.backpressure_events if e.event_type == 'DataRead')
        increment_window_count = sum(1 for e in self.backpressure_events if e.event_type == 'IncrementReadWindow')
        print(f"\n📊 Backpressure Events Summary:")
        print(f"  Total events: {colored(str(total_backpressure_events), Colors.WHITE, bold=True)}")
        print(f"  📖 DataRead events: {colored(str(data_read_count), Colors.GREEN, bold=True)}")
        print(f"  📈 IncrementReadWindow events: {colored(str(increment_window_count), Colors.BLUE, bold=True)}")


def main():
    """Main function to find and display peak memory usage."""
    # Disable colors if output is not a terminal
    if not sys.stdout.isatty():
        Colors.disable()

    if len(sys.argv) < 2:
        print(colored("Usage:", Colors.YELLOW, bold=True) + " python peak_buffer_memory.py <log_file>")
        print(colored("Example:", Colors.YELLOW, bold=True) + " python peak_buffer_memory.py logs/mountpoint-s3.log")
        sys.exit(1)

    log_file = sys.argv[1]

    # Initialize analyzer and parse log
    analyzer = PeakMemoryAnalyzer()
    print(f"🔍 Parsing log file: {colored(log_file, Colors.BLUE, bold=True)}")
    analyzer.parse_log_file(log_file)

    # Print summary
    analyzer.print_summary()
    print()

    if not analyzer.events:
        print(colored("❌ No buffer allocation events found in log file.", Colors.RED, bold=True))
        return

    # Find peak memory usage
    print(colored("🔍 ANALYZING MEMORY USAGE OVER TIME...", Colors.MAGENTA, bold=True))
    peak_timestamp, peak_memory, peak_allocations = analyzer.find_peak_memory_usage()
    read_windows = analyzer.get_read_window_at_timestamp(peak_timestamp)

    if peak_memory == 0:
        print(colored("❌ No memory usage detected.", Colors.RED, bold=True))
        return

    print(f"\n{colored('🏔️  PEAK MEMORY USAGE FOUND', Colors.RED, bold=True)}")
    print("=" * 70)
    print(f"📅 Timestamp: {colored(str(peak_timestamp), Colors.BLUE, bold=True)}")
    print(f"💾 Peak Memory: {format_bytes(peak_memory)}")
    print()

    # Show allocations at peak time
    print(colored("🎯 ALLOCATIONS AT PEAK MEMORY USAGE", Colors.MAGENTA, bold=True))
    print("-" * 70)

    if not peak_allocations:
        print(colored("❌ No allocations found at peak timestamp.", Colors.RED, bold=True))
    else:
        for s3_key, ranges in peak_allocations.items():
            print(f"\n🗂️  S3 Key: {colored(s3_key, Colors.CYAN, bold=True)}")
            total_size = 0
            for i, (offset, size) in enumerate(ranges, 1):
                range_str = f"[{offset:,}-{offset+size-1:,}]"
                print(f"  📦 Buffer {i}: Offset {colored(f'{offset:,}', Colors.WHITE, bold=True)} "
                      f"Size {format_bytes(size)} Range {colored(range_str, Colors.GREEN)}")
                total_size += size
            print(f"  📊 Total for key: {format_bytes(total_size)}")
            if s3_key in read_windows:
                (start_offset, end_offset, window_size) = read_windows[s3_key]
                if window_size:
                    print(f"  📏 Window Size: {format_bytes(window_size)}")
                if end_offset is None:
                    end_offset = 0
                window_range = f"[{start_offset:,}-{end_offset-1:,}]"
                print(f"  📐 Window Range: {colored(window_range, Colors.GREEN)}")

    # Calculate and display total read window size
    if read_windows:
        total_read_window_size = 0
        for object_name, (start_offset, end_offset, window_size) in read_windows.items():
            if window_size is not None:
                total_read_window_size += window_size

        print(f"\n{colored('📊 TOTAL READ WINDOW SIZE', Colors.MAGENTA, bold=True)}")
        print("-" * 70)
        print(f"Total Size: {format_bytes(total_read_window_size)}")


if __name__ == "__main__":
    main()
