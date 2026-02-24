"""Data normalization and cleaning for OpenWeather payloads."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from weather_etl.ingestion.models.types import DailyForecastRecord, HourlyForecastRecord


def normalize_hourly_4d(
    payload: dict[str, Any], extracted_at: datetime
) -> list[HourlyForecastRecord]:
    """Normalize the 4-day hourly payload into typed records."""
    city = payload.get("city", {})
    coord = city.get("coord", {})
    rows: list[HourlyForecastRecord] = []
    for item in payload.get("list", []):
        weather = _first_weather(item.get("weather", []))
        main = item.get("main", {})
        rain = item.get("rain", {})
        wind = item.get("wind", {})
        clouds = item.get("clouds", {})
        forecast_at = _epoch_to_dt(item.get("dt"))
        if forecast_at is None:
            continue

        rows.append(
            HourlyForecastRecord(
                location_name=str(city.get("name", "")),
                country_code=str(city.get("country", "")),
                lat=float(coord.get("lat", 0)),
                lon=float(coord.get("lon", 0)),
                forecast_at_utc=forecast_at,
                temperature_c=float(main.get("temp", 0)),
                feels_like_c=float(main.get("feels_like", 0)),
                temp_min_c=float(main.get("temp_min", 0)),
                temp_max_c=float(main.get("temp_max", 0)),
                pressure_hpa=int(main.get("pressure", 0)),
                sea_level_hpa=_to_int_or_none(main.get("sea_level")),
                ground_level_hpa=_to_int_or_none(main.get("grnd_level")),
                humidity_pct=int(main.get("humidity", 0)),
                cloudiness_pct=int(clouds.get("all", 0)),
                wind_speed_ms=float(wind.get("speed", 0)),
                wind_deg=int(wind.get("deg", 0)),
                wind_gust_ms=_to_float_or_none(wind.get("gust")),
                visibility_m=_to_int_or_none(item.get("visibility")),
                precipitation_probability=float(item.get("pop", 0)),
                rain_1h_mm=float(rain.get("1h", 0) or 0),
                weather_code=int(weather.get("id", 0)),
                weather_main=str(weather.get("main", "")),
                weather_description=str(weather.get("description", "")),
                weather_icon=str(weather.get("icon", "")),
                pod=item.get("sys", {}).get("pod"),
                source_payload_ts=extracted_at,
            )
        )
    return rows


def normalize_daily_30d(
    payload: dict[str, Any], extracted_at: datetime
) -> list[DailyForecastRecord]:
    """Normalize the 30-day daily payload into typed records."""
    city = payload.get("city", {})
    coord = city.get("coord", {})
    rows: list[DailyForecastRecord] = []
    for item in payload.get("list", []):
        weather = _first_weather(item.get("weather", []))
        temp = item.get("temp", {})
        feels = item.get("feels_like", {})
        forecast_at = _epoch_to_dt(item.get("dt"))
        if forecast_at is None:
            continue
        rows.append(
            DailyForecastRecord(
                location_name=str(city.get("name", "")),
                country_code=str(city.get("country", "")),
                lat=float(coord.get("lat", 0)),
                lon=float(coord.get("lon", 0)),
                forecast_date=forecast_at.date(),
                sunrise_utc=_epoch_to_dt(item.get("sunrise")),
                sunset_utc=_epoch_to_dt(item.get("sunset")),
                temp_day_c=float(temp.get("day", 0)),
                temp_min_c=float(temp.get("min", 0)),
                temp_max_c=float(temp.get("max", 0)),
                temp_night_c=float(temp.get("night", 0)),
                temp_evening_c=float(temp.get("eve", 0)),
                temp_morning_c=float(temp.get("morn", 0)),
                feels_like_day_c=_to_float_or_none(feels.get("day")),
                feels_like_night_c=_to_float_or_none(feels.get("night")),
                feels_like_evening_c=_to_float_or_none(feels.get("eve")),
                feels_like_morning_c=_to_float_or_none(feels.get("morn")),
                pressure_hpa=int(item.get("pressure", 0)),
                humidity_pct=int(item.get("humidity", 0)),
                wind_speed_ms=float(item.get("speed", 0)),
                wind_deg=int(item.get("deg", 0)),
                cloudiness_pct=int(item.get("clouds", 0)),
                rain_mm=float(item.get("rain", 0) or 0),
                weather_code=int(weather.get("id", 0)),
                weather_main=str(weather.get("main", "")),
                weather_description=str(weather.get("description", "")),
                weather_icon=str(weather.get("icon", "")),
                source_payload_ts=extracted_at,
            )
        )
    return rows


def _first_weather(values: list[dict[str, Any]]) -> dict[str, Any]:
    return values[0] if values else {}


def _epoch_to_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    return datetime.fromtimestamp(int(value), tz=UTC)


def _to_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _to_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)
