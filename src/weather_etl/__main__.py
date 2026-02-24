"""CLI entrypoint for weather ETL execution modes."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from functools import cache

import typer

from weather_etl.common.config import Settings
from weather_etl.common.logger import configure_console_logging
from weather_etl.ingestion.openweather_client import OpenWeatherClient
from weather_etl.ingestion.ops.normalize import normalize_daily_30d, normalize_hourly_4d
from weather_etl.ingestion.ops.postgres_loader import PostgresLoader

app = typer.Typer()
logger = logging.getLogger("weather_etl")


@cache
def _get_settings() -> Settings:
    """Load settings (cached)."""
    return Settings.from_env()


def _get_client() -> OpenWeatherClient:
    """Create an OpenWeatherClient using current settings."""
    settings = _get_settings()
    return OpenWeatherClient(
        api_key=settings.api_key,
        timeout_s=settings.request_timeout_s,
        max_retries=settings.max_retries,
        backoff_initial_s=settings.backoff_initial_s,
        backoff_max_s=settings.backoff_max_s,
        min_interval_s=settings.api_min_interval_s,
    )


def _get_loader() -> PostgresLoader:
    """Create a PostgresLoader using current settings."""
    settings = _get_settings()
    return PostgresLoader(settings.db_dsn)


@app.command()
def run_hourly() -> None:
    """
    Extract, transform, and load the 4-day hourly forecast.
    """
    extracted_at = datetime.now(tz=UTC)
    settings = _get_settings()
    client = _get_client()
    loader = _get_loader()
    try:
        loader.init_schema()
        raw_hourly = client.fetch_hourly_4d(settings.lat, settings.lon, settings.units)
        hourly_rows = normalize_hourly_4d(raw_hourly, extracted_at)
        loaded = loader.upsert_hourly(hourly_rows)
        logger.info(f"Loaded {loaded} hourly rows")
    finally:
        client.close()


@app.command()
def run_daily() -> None:
    """
    Extract, transform, and load the 30-day daily forecast.
    """
    extracted_at = datetime.now(tz=UTC)
    settings = _get_settings()
    client = _get_client()
    loader = _get_loader()
    try:
        loader.init_schema()
        raw_daily = client.fetch_daily_30d(settings.lat, settings.lon, settings.units)
        daily_rows = normalize_daily_30d(raw_daily, extracted_at)
        loaded = loader.upsert_daily(daily_rows)
        logger.info(f"Loaded {loaded} daily rows")
    finally:
        client.close()


def main() -> None:
    """
    Main entrypoint for the weather ETL pipeline.
    """
    configure_console_logging()
    try:
        app()
    except SystemExit as exc:
        if exc.code != 0:
            logger.warning(f"Error: {str(exc)}")
            raise


if __name__ == "__main__":
    main()
