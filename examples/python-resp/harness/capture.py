from __future__ import annotations

import io
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


class _Tee(io.TextIOBase):
    def __init__(self, *streams: io.TextIOBase) -> None:
        self._streams = streams

    def write(self, s: str) -> int:  # type: ignore[override]
        for st in self._streams:
            st.write(s)
            st.flush()
        return len(s)

    def flush(self) -> None:
        for st in self._streams:
            st.flush()


@contextmanager
def capture_transcript(log_path: Path) -> Iterator[io.StringIO]:
    """Tee stdout/stderr to a log file and an in-memory buffer."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    buf = io.StringIO()
    with log_path.open("w", encoding="utf-8") as fh:
        tee_out = _Tee(sys.__stdout__, fh, buf)  # type: ignore[arg-type]
        tee_err = _Tee(sys.__stderr__, fh, buf)  # type: ignore[arg-type]
        old_out, old_err = sys.stdout, sys.stderr
        sys.stdout, sys.stderr = tee_out, tee_err  # type: ignore[assignment]
        try:
            yield buf
        finally:
            sys.stdout, sys.stderr = old_out, old_err
