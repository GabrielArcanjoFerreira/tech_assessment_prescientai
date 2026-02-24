CREATE SCHEMA IF NOT EXISTS weather;

CREATE TABLE IF NOT EXISTS weather.hourly_forecast (
    id BIGSERIAL PRIMARY KEY,
    location_name TEXT NOT NULL,
    country_code VARCHAR(2) NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    forecast_at_utc TIMESTAMPTZ NOT NULL,
    temperature_c DOUBLE PRECISION NOT NULL,
    feels_like_c DOUBLE PRECISION NOT NULL,
    temp_min_c DOUBLE PRECISION NOT NULL,
    temp_max_c DOUBLE PRECISION NOT NULL,
    pressure_hpa INTEGER NOT NULL CHECK (pressure_hpa > 0),
    sea_level_hpa INTEGER,
    ground_level_hpa INTEGER,
    humidity_pct INTEGER NOT NULL CHECK (humidity_pct BETWEEN 0 AND 100),
    cloudiness_pct INTEGER NOT NULL CHECK (cloudiness_pct BETWEEN 0 AND 100),
    wind_speed_ms DOUBLE PRECISION NOT NULL CHECK (wind_speed_ms >= 0),
    wind_deg INTEGER NOT NULL CHECK (wind_deg BETWEEN 0 AND 360),
    wind_gust_ms DOUBLE PRECISION,
    visibility_m INTEGER,
    precipitation_probability DOUBLE PRECISION NOT NULL CHECK (precipitation_probability BETWEEN 0 AND 1),
    rain_1h_mm DOUBLE PRECISION NOT NULL CHECK (rain_1h_mm >= 0),
    weather_code INTEGER NOT NULL,
    weather_main TEXT NOT NULL,
    weather_description TEXT NOT NULL,
    weather_icon VARCHAR(4) NOT NULL,
    pod VARCHAR(1),
    source_payload_ts TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (lat, lon, forecast_at_utc)
);

CREATE INDEX IF NOT EXISTS idx_hourly_forecast_at
    ON weather.hourly_forecast (forecast_at_utc);

CREATE TABLE IF NOT EXISTS weather.daily_forecast (
    id BIGSERIAL PRIMARY KEY,
    location_name TEXT NOT NULL,
    country_code VARCHAR(2) NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    forecast_date DATE NOT NULL,
    sunrise_utc TIMESTAMPTZ,
    sunset_utc TIMESTAMPTZ,
    temp_day_c DOUBLE PRECISION NOT NULL,
    temp_min_c DOUBLE PRECISION NOT NULL,
    temp_max_c DOUBLE PRECISION NOT NULL,
    temp_night_c DOUBLE PRECISION NOT NULL,
    temp_evening_c DOUBLE PRECISION NOT NULL,
    temp_morning_c DOUBLE PRECISION NOT NULL,
    feels_like_day_c DOUBLE PRECISION,
    feels_like_night_c DOUBLE PRECISION,
    feels_like_evening_c DOUBLE PRECISION,
    feels_like_morning_c DOUBLE PRECISION,
    pressure_hpa INTEGER NOT NULL CHECK (pressure_hpa > 0),
    humidity_pct INTEGER NOT NULL CHECK (humidity_pct BETWEEN 0 AND 100),
    wind_speed_ms DOUBLE PRECISION NOT NULL CHECK (wind_speed_ms >= 0),
    wind_deg INTEGER NOT NULL CHECK (wind_deg BETWEEN 0 AND 360),
    cloudiness_pct INTEGER NOT NULL CHECK (cloudiness_pct BETWEEN 0 AND 100),
    rain_mm DOUBLE PRECISION NOT NULL CHECK (rain_mm >= 0),
    weather_code INTEGER NOT NULL,
    weather_main TEXT NOT NULL,
    weather_description TEXT NOT NULL,
    weather_icon VARCHAR(4) NOT NULL,
    source_payload_ts TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (lat, lon, forecast_date)
);

CREATE INDEX IF NOT EXISTS idx_daily_forecast_date
    ON weather.daily_forecast (forecast_date);
