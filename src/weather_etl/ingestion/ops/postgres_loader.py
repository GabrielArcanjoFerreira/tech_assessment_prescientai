"""PostgreSQL loading utilities for normalized weather records."""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

import psycopg

from weather_etl.ingestion.models.types import DailyForecastRecord, HourlyForecastRecord


class PostgresLoader:
    """Responsible for schema bootstrap and idempotent upserts."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def init_schema(self) -> None:
        """Create DB tables, constraints, and indexes if missing."""
        schema_path = Path(__file__).resolve().parents[2] / "sql" / "schema.sql"
        ddl = schema_path.read_text(encoding="utf-8")
        with psycopg.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()

    def upsert_hourly(self, rows: list[HourlyForecastRecord]) -> int:
        """Upsert hourly records using natural unique key."""
        if not rows:
            return 0
        sql = """
            INSERT INTO weather.hourly_forecast (
                location_name, country_code, lat, lon, forecast_at_utc,
                temperature_c, feels_like_c, temp_min_c, temp_max_c,
                pressure_hpa, sea_level_hpa, ground_level_hpa, humidity_pct,
                cloudiness_pct, wind_speed_ms, wind_deg, wind_gust_ms,
                visibility_m, precipitation_probability, rain_1h_mm,
                weather_code, weather_main, weather_description, weather_icon,
                pod, source_payload_ts
            ) VALUES (
                %(location_name)s, %(country_code)s, %(lat)s, %(lon)s, %(forecast_at_utc)s,
                %(temperature_c)s, %(feels_like_c)s, %(temp_min_c)s, %(temp_max_c)s,
                %(pressure_hpa)s, %(sea_level_hpa)s, %(ground_level_hpa)s, %(humidity_pct)s,
                %(cloudiness_pct)s, %(wind_speed_ms)s, %(wind_deg)s, %(wind_gust_ms)s,
                %(visibility_m)s, %(precipitation_probability)s, %(rain_1h_mm)s,
                %(weather_code)s, %(weather_main)s, %(weather_description)s, %(weather_icon)s,
                %(pod)s, %(source_payload_ts)s
            )
            ON CONFLICT (lat, lon, forecast_at_utc) DO UPDATE
            SET
                temperature_c = EXCLUDED.temperature_c,
                feels_like_c = EXCLUDED.feels_like_c,
                temp_min_c = EXCLUDED.temp_min_c,
                temp_max_c = EXCLUDED.temp_max_c,
                pressure_hpa = EXCLUDED.pressure_hpa,
                sea_level_hpa = EXCLUDED.sea_level_hpa,
                ground_level_hpa = EXCLUDED.ground_level_hpa,
                humidity_pct = EXCLUDED.humidity_pct,
                cloudiness_pct = EXCLUDED.cloudiness_pct,
                wind_speed_ms = EXCLUDED.wind_speed_ms,
                wind_deg = EXCLUDED.wind_deg,
                wind_gust_ms = EXCLUDED.wind_gust_ms,
                visibility_m = EXCLUDED.visibility_m,
                precipitation_probability = EXCLUDED.precipitation_probability,
                rain_1h_mm = EXCLUDED.rain_1h_mm,
                weather_code = EXCLUDED.weather_code,
                weather_main = EXCLUDED.weather_main,
                weather_description = EXCLUDED.weather_description,
                weather_icon = EXCLUDED.weather_icon,
                pod = EXCLUDED.pod,
                source_payload_ts = EXCLUDED.source_payload_ts;
        """
        payload = [asdict(row) for row in rows]
        with psycopg.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, payload)
            conn.commit()
        return len(rows)

    def upsert_daily(self, rows: list[DailyForecastRecord]) -> int:
        """Upsert daily records using natural unique key."""
        if not rows:
            return 0
        sql = """
            INSERT INTO weather.daily_forecast (
                location_name, country_code, lat, lon, forecast_date,
                sunrise_utc, sunset_utc, temp_day_c, temp_min_c, temp_max_c,
                temp_night_c, temp_evening_c, temp_morning_c,
                feels_like_day_c, feels_like_night_c,
                feels_like_evening_c, feels_like_morning_c,
                pressure_hpa, humidity_pct, wind_speed_ms,
                wind_deg, cloudiness_pct,
                rain_mm, weather_code, weather_main,
                weather_description, weather_icon,
                source_payload_ts
            ) VALUES (
                %(location_name)s, %(country_code)s, %(lat)s, %(lon)s, %(forecast_date)s,
                %(sunrise_utc)s, %(sunset_utc)s, %(temp_day_c)s, %(temp_min_c)s, %(temp_max_c)s,
                %(temp_night_c)s, %(temp_evening_c)s, %(temp_morning_c)s,
                %(feels_like_day_c)s, %(feels_like_night_c)s,
                %(feels_like_evening_c)s, %(feels_like_morning_c)s,
                %(pressure_hpa)s, %(humidity_pct)s, %(wind_speed_ms)s,
                %(wind_deg)s, %(cloudiness_pct)s,
                %(rain_mm)s, %(weather_code)s, %(weather_main)s,
                %(weather_description)s, %(weather_icon)s,
                %(source_payload_ts)s
            )
            ON CONFLICT (lat, lon, forecast_date) DO UPDATE
            SET
                sunrise_utc = EXCLUDED.sunrise_utc,
                sunset_utc = EXCLUDED.sunset_utc,
                temp_day_c = EXCLUDED.temp_day_c,
                temp_min_c = EXCLUDED.temp_min_c,
                temp_max_c = EXCLUDED.temp_max_c,
                temp_night_c = EXCLUDED.temp_night_c,
                temp_evening_c = EXCLUDED.temp_evening_c,
                temp_morning_c = EXCLUDED.temp_morning_c,
                feels_like_day_c = EXCLUDED.feels_like_day_c,
                feels_like_night_c = EXCLUDED.feels_like_night_c,
                feels_like_evening_c = EXCLUDED.feels_like_evening_c,
                feels_like_morning_c = EXCLUDED.feels_like_morning_c,
                pressure_hpa = EXCLUDED.pressure_hpa,
                humidity_pct = EXCLUDED.humidity_pct,
                wind_speed_ms = EXCLUDED.wind_speed_ms,
                wind_deg = EXCLUDED.wind_deg,
                cloudiness_pct = EXCLUDED.cloudiness_pct,
                rain_mm = EXCLUDED.rain_mm,
                weather_code = EXCLUDED.weather_code,
                weather_main = EXCLUDED.weather_main,
                weather_description = EXCLUDED.weather_description,
                weather_icon = EXCLUDED.weather_icon,
                source_payload_ts = EXCLUDED.source_payload_ts;
        """
        payload = [asdict(row) for row in rows]
        with psycopg.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, payload)
            conn.commit()
        return len(rows)
