from __future__ import annotations

from datetime import UTC, datetime

from weather_etl.ingestion.ops.transform.normalize import normalize_daily_30d, normalize_hourly_4d


def test_normalize_hourly_4d_minimal_payload() -> None:
    payload = {
        "city": {
            "name": "El Colorado",
            "country": "CL",
            "coord": {"lat": -33.3496, "lon": -70.2922},
        },
        "list": [
            {
                "dt": 1661875200,
                "main": {
                    "temp": 2.5,
                    "feels_like": 1.9,
                    "temp_min": 2.2,
                    "temp_max": 2.8,
                    "pressure": 1015,
                    "humidity": 50,
                },
                "weather": [
                    {
                        "id": 500,
                        "main": "Rain",
                        "description": "light rain",
                        "icon": "10d",
                    }
                ],
                "clouds": {"all": 97},
                "wind": {"speed": 1.06, "deg": 66, "gust": 2.16},
                "visibility": 10000,
                "pop": 0.32,
                "rain": {"1h": 0.13},
                "sys": {"pod": "d"},
            }
        ],
    }
    rows = normalize_hourly_4d(payload, extracted_at=datetime.now(tz=UTC))
    assert len(rows) == 1
    assert rows[0].location_name == "El Colorado"
    assert rows[0].rain_1h_mm == 0.13
    assert rows[0].weather_main == "Rain"


def test_normalize_daily_30d_minimal_payload() -> None:
    payload = {
        "city": {
            "name": "El Colorado",
            "country": "CL",
            "coord": {"lat": -33.3496, "lon": -70.2922},
        },
        "list": [
            {
                "dt": 1594382400,
                "sunrise": 1594353335,
                "sunset": 1594412149,
                "temp": {
                    "day": 5.1,
                    "min": 1.2,
                    "max": 6.7,
                    "night": 1.8,
                    "eve": 4.9,
                    "morn": 2.3,
                },
                "feels_like": {"day": 1.5, "night": -1.0, "eve": 0.2, "morn": 0.4},
                "pressure": 1016,
                "humidity": 84,
                "weather": [
                    {
                        "id": 500,
                        "main": "Rain",
                        "description": "light rain",
                        "icon": "10d",
                    }
                ],
                "speed": 6.78,
                "deg": 320,
                "clouds": 81,
                "rain": 1.96,
            }
        ],
    }
    rows = normalize_daily_30d(payload, extracted_at=datetime.now(tz=UTC))
    assert len(rows) == 1
    assert rows[0].forecast_date.isoformat() == "2020-07-10"
    assert rows[0].temp_day_c == 5.1
    assert rows[0].weather_description == "light rain"
