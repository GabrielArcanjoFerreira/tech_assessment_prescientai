"""Typed domain records used across ETL steps."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True, slots=True)
class HourlyForecastRecord:
    """Normalized record for the 4-day hourly forecast feed."""

    location_name: str
    country_code: str
    lat: float
    lon: float
    forecast_at_utc: datetime
    temperature_c: float
    feels_like_c: float
    temp_min_c: float
    temp_max_c: float
    pressure_hpa: int
    sea_level_hpa: int | None
    ground_level_hpa: int | None
    humidity_pct: int
    cloudiness_pct: int
    wind_speed_ms: float
    wind_deg: int
    wind_gust_ms: float | None
    visibility_m: int | None
    precipitation_probability: float
    rain_1h_mm: float
    weather_code: int
    weather_main: str
    weather_description: str
    weather_icon: str
    pod: str | None
    source_payload_ts: datetime


@dataclass(frozen=True, slots=True)
class DailyForecastRecord:
    """Normalized record for the 30-day daily climate forecast feed."""

    location_name: str
    country_code: str
    lat: float
    lon: float
    forecast_date: date
    sunrise_utc: datetime | None
    sunset_utc: datetime | None
    temp_day_c: float
    temp_min_c: float
    temp_max_c: float
    temp_night_c: float
    temp_evening_c: float
    temp_morning_c: float
    feels_like_day_c: float | None
    feels_like_night_c: float | None
    feels_like_evening_c: float | None
    feels_like_morning_c: float | None
    pressure_hpa: int
    humidity_pct: int
    wind_speed_ms: float
    wind_deg: int
    cloudiness_pct: int
    rain_mm: float
    weather_code: int
    weather_main: str
    weather_description: str
    weather_icon: str
    source_payload_ts: datetime
