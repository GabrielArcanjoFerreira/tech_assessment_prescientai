"""OpenWeather API client with retry, timeout and rate-limit controls."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from weather_etl.common.rate_limit import RateLimiter

logger = logging.getLogger("weather_etl")

FOUR_DAY_HOURLY_ENDPOINT = "/data/2.5/forecast/hourly"
THIRTY_DAY_DAILY_ENDPOINT = "/data/2.5/forecast/climate"


@dataclass(slots=True)
class OpenWeatherClient:
    """Resilient client wrapper for OpenWeather Pro endpoints."""

    api_key: str
    base_url: str = "https://pro.openweathermap.org"
    timeout_s: float = 20.0
    max_retries: int = 5
    backoff_initial_s: float = 1.0
    backoff_max_s: float = 30.0
    min_interval_s: float = 1.0
    _http: httpx.Client = field(init=False)
    _rate_limiter: RateLimiter = field(init=False)

    def __post_init__(self) -> None:
        self._http = httpx.Client(base_url=self.base_url, timeout=self.timeout_s)
        self._rate_limiter = RateLimiter(self.min_interval_s)

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._http.close()

    def _request_json(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        """GET JSON with retries and backoff handling for transient errors."""
        attempt = 0
        wait_s = self.backoff_initial_s
        while True:
            attempt += 1
            self._rate_limiter.wait()
            try:
                response = self._http.get(path, params=params)
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        sleep_s = float(retry_after)
                    else:
                        sleep_s = wait_s
                    logger.warning(
                        "Rate limit reached. Sleeping %.2fs before retry.",
                        sleep_s,
                    )
                    time.sleep(min(sleep_s, self.backoff_max_s))
                    if attempt <= self.max_retries:
                        wait_s = min(wait_s * 2, self.backoff_max_s)
                        continue

                response.raise_for_status()
                payload: dict[str, Any] = response.json()
                if payload.get("cod") not in ("200", 200):
                    raise ValueError(f"OpenWeather returned non-success cod: {payload.get('cod')}")
                return payload

            except (httpx.RequestError, httpx.HTTPStatusError, ValueError) as exc:
                if attempt > self.max_retries:
                    raise RuntimeError(f"API request failed after {attempt} attempts") from exc
                logger.warning(
                    "API request failure on attempt %s/%s: %s",
                    attempt,
                    self.max_retries,
                    exc,
                )
                time.sleep(wait_s)
                wait_s = min(wait_s * 2, self.backoff_max_s)

    def fetch_hourly_4d(self, lat: float, lon: float, units: str = "metric") -> dict[str, Any]:
        """Fetch next 4 days hourly forecast."""
        return self._request_json(
            FOUR_DAY_HOURLY_ENDPOINT,
            params={
                "lat": lat,
                "lon": lon,
                "appid": self.api_key,
                "units": units,
            },
        )

    def fetch_daily_30d(self, lat: float, lon: float, units: str = "metric") -> dict[str, Any]:
        """Fetch next 30 days daily forecast."""
        return self._request_json(
            THIRTY_DAY_DAILY_ENDPOINT,
            params={
                "lat": lat,
                "lon": lon,
                "appid": self.api_key,
                "units": units,
            },
        )
