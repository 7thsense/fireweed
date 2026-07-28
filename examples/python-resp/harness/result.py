from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ScenarioResult:
    status: str  # pass | fail | skip
    details: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    @classmethod
    def ok(cls, **details: Any) -> ScenarioResult:
        return cls(status="pass", details=dict(details))

    @classmethod
    def skip(cls, reason: str, **details: Any) -> ScenarioResult:
        return cls(status="skip", details=dict(details), error=reason)

    @classmethod
    def fail(cls, message: str, **details: Any) -> ScenarioResult:
        return cls(status="fail", details=dict(details), error=message)
