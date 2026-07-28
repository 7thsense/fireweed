from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import redis

from lib import resp as R


@dataclass
class ScenarioContext:
    redis: redis.Redis
    queue: str
    run_id: str
    evidence_dir: str
    full: bool = False
    # Perf knobs (ignored by functional scenarios)
    perf_n: int = 1_000_000
    perf_pipeline: int = 1000
    perf_claim_count: int = 1000
    group: str = R.DEFAULT_GROUP
    consumer: str = R.DEFAULT_CONSUMER
    _checks: list[str] = field(default_factory=list)

    def key(self, name: str) -> str:
        """Stable client_item_key unique to this run."""
        return f"{self.run_id}-{name}"

    def now_ms(self) -> int:
        return R.now_ms()

    def check(self, condition: bool, message: str) -> None:
        if not condition:
            raise AssertionError(message)
        self._checks.append(message)

    def log(self, msg: str) -> None:
        print(msg, flush=True)
