"""Simple in-process rate limiter for API calls."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from threading import Lock


@dataclass(slots=True)
class RateLimiter:
    """Ensure a minimum interval between subsequent API calls."""

    min_interval_s: float
    _last_call: float = 0.0
    _lock: Lock = field(default_factory=Lock)

    def wait(self) -> None:
        """Sleep when needed to honor minimum call interval."""
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_call
            wait_s = self.min_interval_s - elapsed
            if wait_s > 0:
                time.sleep(wait_s)
            self._last_call = time.monotonic()
