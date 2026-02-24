from __future__ import annotations

from typing import Any

import httpx
import pytest

from weather_etl.ingestion.openweather_client import OpenWeatherClient


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        payload: dict[str, Any],
        headers: dict[str, str] | None = None,
    ):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            request = httpx.Request("GET", "https://pro.openweathermap.org")
            response = httpx.Response(self.status_code, request=request)
            raise httpx.HTTPStatusError("error", request=request, response=response)

    def json(self) -> dict[str, Any]:
        return self._payload


def test_client_retries_on_429_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    client = OpenWeatherClient(api_key="k", max_retries=3, backoff_initial_s=0, backoff_max_s=0)
    calls = {"n": 0}

    def fake_get(_url: str, params: dict[str, Any]) -> FakeResponse:
        del _url, params
        calls["n"] += 1
        if calls["n"] == 1:
            return FakeResponse(429, {"cod": "429"}, headers={"Retry-After": "0"})
        return FakeResponse(200, {"cod": "200", "list": [], "city": {}})

    monkeypatch.setattr(client._http, "get", fake_get)
    out = client.fetch_hourly_4d(-33.3, -70.2)
    client.close()
    assert out["cod"] == "200"
    assert calls["n"] == 2


def test_client_raises_after_max_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    client = OpenWeatherClient(api_key="k", max_retries=1, backoff_initial_s=0, backoff_max_s=0)

    def fake_get(_url: str, params: dict[str, Any]) -> FakeResponse:
        del _url, params
        return FakeResponse(500, {"cod": "500"})

    monkeypatch.setattr(client._http, "get", fake_get)
    with pytest.raises(RuntimeError):
        client.fetch_daily_30d(-33.3, -70.2)
    client.close()
