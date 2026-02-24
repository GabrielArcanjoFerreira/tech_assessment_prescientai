# Weather ETL for El Colorado (OpenWeather -> PostgreSQL)

Python ETL pipeline to extract OpenWeather forecasts (4-day hourly + 30-day daily), normalize payloads, and load them into PostgreSQL with idempotent upserts.

## Current Features

- CLI with two commands: `weather-etl run-hourly` and `weather-etl run-daily`.
- Resilient HTTP client with timeout, exponential retry, `429` handling, and local rate limiting.
- Normalization into typed records (`dataclass`) before loading.
- Schema/table bootstrap and upserts with `ON CONFLICT`.

## Architecture (Current Paths)

- `src/weather_etl/common/config.py`: environment variable loading/validation.
- `src/weather_etl/common/logger.py`: console logging setup.
- `src/weather_etl/common/rate_limit.py`: simple minimum-interval rate limiter.
- `src/weather_etl/ingestion/openweather_client.py`: OpenWeather Pro client.
- `src/weather_etl/ingestion/ops/transform/normalize.py`: raw payload transformation into typed records.
- `src/weather_etl/ingestion/ops/load/postgres_loader.py`: schema initialization and PostgreSQL upserts.
- `src/weather_etl/ingestion/models/types.py`: typed contracts (`HourlyForecastRecord`, `DailyForecastRecord`).
- `src/weather_etl/sql/schema.sql`: DDL for `weather.hourly_forecast` and `weather.daily_forecast`.
- `src/weather_etl/__main__.py`: CLI entrypoint.

For a full architecture deep dive (including Databricks Asset Bundles/Jobs plan and ERD), see [Solution Architecture Documentation](docs/solution-architecture.md).

## Quick Start

1. Create and activate a virtual environment:
   - `uv venv .venv`
   - `source .venv/bin/activate`
2. Install dependencies (using Makefile targets):
   - Development setup: `make install-dev-local`
3. Configure environment:
   - `cp .env.example .env`
   - Set `OPENWEATHER_API_KEY`
   - Set `WEATHER_DB_DSN` with valid PostgreSQL user/password
4. Run ETL commands:
   - Hourly (4-day forecast): `weather-etl run-hourly`
   - Daily (30-day forecast): `weather-etl run-daily`
5. Run tests:
   - `make test`
6. Build distribution artifacts:
   - `make dist`

Important: do not run plain `make` unless you want cleanup only, because the default Make target is `clean`.

## Makefile Targets

- `make install-dev-local`: install project + dev dependencies + `pre-commit`.
- `make install`: clean artifacts, then install runtime dependencies.
- `make test`: run test suite with coverage report.
- `make dist`: clean artifacts, build source/wheel distribution, and list `dist/`.
- `make clean`: remove build, cache, coverage, and Python artifacts.
- `make clean-build` / `make clean-pyc` / `make clean-test`: run only specific cleanup scopes.

Note: the default Make target is `clean`, so running only `make` will clean the repo.

## Environment Variables

Template is available in `.env.example`:

- `OPENWEATHER_API_KEY` (required)
- `OPENWEATHER_LAT` and `OPENWEATHER_LON` (required)
- `OPENWEATHER_UNITS` (default: `metric`)
- `WEATHER_DB_DSN` (default: `postgresql://postgres:postgres@localhost:5432/weather`)
- `WEATHER_LOG_LEVEL` (default: `INFO`)
- `WEATHER_REQUEST_TIMEOUT_S` (default: `20`)
- `WEATHER_API_MIN_INTERVAL_S` (default: `1`)
- `WEATHER_MAX_RETRIES` (default: `5`)
- `WEATHER_BACKOFF_INITIAL_S` (default: `1`)
- `WEATHER_BACKOFF_MAX_S` (default: `30`)

## Quality and Tests

- Full local checks via hooks: `pre-commit run -a`
- Run tests via Makefile: `make test`
- Equivalent direct test command: `uv run pytest -q --cov=src/weather_etl --cov-report=term-missing`

## Data Model (PostgreSQL)

Schema: `weather`.

- `weather.hourly_forecast`
  - natural unique key: `(lat, lon, forecast_at_utc)`
  - index: `idx_hourly_forecast_at`
  - quality checks (e.g., humidity 0-100, pop 0-1, rain >= 0)
- `weather.daily_forecast`
  - natural unique key: `(lat, lon, forecast_date)`
  - index: `idx_daily_forecast_date`
  - equivalent quality checks

Full DDL is in `src/weather_etl/sql/schema.sql`.

## Quick Troubleshooting

- Error `password authentication failed for user "postgres"`:
  - review `WEATHER_DB_DSN` in `.env`
  - validate `psql` connectivity before running ETL
- API error (`cod` different from `200`):
  - validate `OPENWEATHER_API_KEY`
  - confirm your account has access to Pro endpoints

## References

- OpenWeather API: https://openweathermap.org/api
- OpenWeather 4-day hourly (`/data/2.5/forecast/hourly`): https://openweathermap.org/forecast5
- OpenWeather 30-day climate (`/data/2.5/forecast/climate`, Pro): https://openweathermap.org/api/forecast30
- Psycopg 3: https://www.psycopg.org/psycopg3/docs/
- Typer: https://typer.tiangolo.com/
- Ruff: https://docs.astral.sh/ruff/
- pre-commit: https://pre-commit.com/
- Python packaging: https://packaging.python.org/en/latest/tutorials/packaging-projects/
