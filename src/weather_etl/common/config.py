"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Settings:
    """Runtime settings for the ETL pipeline."""

    api_key: str
    lat: float
    lon: float
    units: str = "metric"
    db_dsn: str = "postgresql://postgres:postgres@localhost:5432/weather"
    log_level: str = "INFO"
    request_timeout_s: float = 20.0
    api_min_interval_s: float = 1.0
    max_retries: int = 5
    backoff_initial_s: float = 1.0
    backoff_max_s: float = 30.0

    @classmethod
    def from_env(cls) -> Settings:
        """Create settings using process environment variables."""
        api_key = os.getenv("OPENWEATHER_API_KEY", "").strip()
        if not api_key:
            raise ValueError("OPENWEATHER_API_KEY is required")
        lat_raw = os.getenv("OPENWEATHER_LAT")
        lon_raw = os.getenv("OPENWEATHER_LON")
        if lat_raw is None or lon_raw is None:
            raise ValueError("OPENWEATHER_LAT and OPENWEATHER_LON are required")

        return cls(
            api_key=api_key,
            lat=float(lat_raw),
            lon=float(lon_raw),
            units=os.getenv("OPENWEATHER_UNITS", "metric"),
            db_dsn=os.getenv(
                "WEATHER_DB_DSN",
                "postgresql://postgres:postgres@localhost:5432/weather",
            ),
            log_level=os.getenv("WEATHER_LOG_LEVEL", "INFO"),
            request_timeout_s=float(os.getenv("WEATHER_REQUEST_TIMEOUT_S", "20")),
            api_min_interval_s=float(os.getenv("WEATHER_API_MIN_INTERVAL_S", "1")),
            max_retries=int(os.getenv("WEATHER_MAX_RETRIES", "5")),
            backoff_initial_s=float(os.getenv("WEATHER_BACKOFF_INITIAL_S", "1")),
            backoff_max_s=float(os.getenv("WEATHER_BACKOFF_MAX_S", "30")),
        )
