"""External WAL footprint sampling; no database connections or storage writes."""
import threading
import time
from pathlib import Path


def process_memory_sample(pid, proc_root=Path('/proc')):
    """Read Linux counters without inspecting memory or touching database files."""
    process = proc_root / str(pid)
    status = {}
    for line in (process / 'status').read_text().splitlines():
        key, _, value = line.partition(':')
        if key in ('VmRSS', 'RssAnon', 'RssFile', 'RssShmem', 'VmSwap', 'VmHWM'):
            status[key + '_kib'] = int(value.split()[0])
    # comm may contain spaces and parentheses; fields after its final ')' start
    # at proc field 3 (state), not field 1 (pid).
    stat = (process / 'stat').read_text()
    fields = stat[stat.rfind(')') + 2:].split()
    status.update(minor_faults=int(fields[7]), major_faults=int(fields[9]),
                  start_ticks=int(fields[19]), monotonic_s=time.monotonic())
    host = {}
    for line in (proc_root / 'meminfo').read_text().splitlines():
        key, _, value = line.partition(':')
        if key in ('MemAvailable', 'Cached', 'SwapFree', 'Dirty', 'Writeback'):
            host[key + '_kib'] = int(value.split()[0])
    status['host'] = host
    return status


class WalMonitor:
    interval_s = 0.1

    def __init__(self, root, pid=None):
        self.root = root
        self.peaks = {}
        self.samples = 0
        self.errors = []
        self.stop_event = threading.Event()
        self.pid = pid
        self.memory_samples = []
        self.memory_errors = []
        self.next_memory_sample = 0.0
        self.thread = threading.Thread(target=self._run, daemon=True)

    def sample(self):
        try:
            for path in self.root.glob("shard-*/projection.db-wal"):
                try:
                    size = path.stat().st_size
                except FileNotFoundError:
                    continue  # A checkpoint or shutdown may remove the file.
                shard = path.parent.name
                self.peaks[shard] = max(self.peaks.get(shard, 0), size)
            self.samples += 1
        except OSError as error:
            if not self.errors:
                self.errors.append(str(error))
        if self.pid is not None and time.monotonic() >= self.next_memory_sample:
            self.next_memory_sample = time.monotonic() + 1.0
            try:
                self.memory_samples.append(process_memory_sample(self.pid))
            except (FileNotFoundError, ProcessLookupError):
                pass  # Normal when the child exits between proc reads.
            except (OSError, ValueError, IndexError) as error:
                if not self.memory_errors:
                    self.memory_errors.append(str(error))

    def _run(self):
        while not self.stop_event.is_set():
            self.sample()
            self.stop_event.wait(self.interval_s)
        self.sample()

    def start(self):
        self.thread.start()

    def finish(self):
        self.stop_event.set()
        self.thread.join()
        return {"interval_ms": int(self.interval_s * 1000), "samples": self.samples,
                "peak_bytes": self.peaks, "errors": self.errors,
                "process_memory": {"interval_ms": 1000, "samples": self.memory_samples,
                                   "errors": self.memory_errors}}
