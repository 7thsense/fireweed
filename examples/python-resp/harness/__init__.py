"""E2E harness: run scenarios, capture transcripts, write evidence."""

from .context import ScenarioContext
from .result import ScenarioResult
from .runner import run_suite

__all__ = ["ScenarioContext", "ScenarioResult", "run_suite"]
