"""External WAL footprint sampling; no database connections or storage writes."""
import threading


class WalMonitor:
    interval_s = 0.1

    def __init__(self, root):
        self.root = root
        self.peaks = {}
        self.samples = 0
        self.errors = []
        self.stop_event = threading.Event()
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
                "peak_bytes": self.peaks, "errors": self.errors}
