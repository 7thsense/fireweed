#!/usr/bin/env python3
"""Alias for: python run_e2e.py --suite perf"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from run_e2e import main

if __name__ == "__main__":
    # Inject --suite perf if not present.
    argv = list(sys.argv[1:])
    if "--suite" not in argv:
        argv = ["--suite", "perf", *argv]
    raise SystemExit(main(argv))
