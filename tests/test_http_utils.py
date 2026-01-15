from types import SimpleNamespace

import requests

from src import http_utils


class DummyLogger:
    def __init__(self) -> None:
        self.messages = []

    def warning(self, message, extra=None):
        self.messages.append((message, extra))


class DummyResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


def test_request_with_retry_on_status(monkeypatch):
    calls = {"count": 0}

    def request(method, url, timeout, **kwargs):
        calls["count"] += 1
        return DummyResponse(500 if calls["count"] == 1 else 200)

    session = SimpleNamespace(request=request)
    monkeypatch.setattr(http_utils.time, "sleep", lambda *_: None)

    response = http_utils.request_with_retry(
        session,
        "GET",
        "https://example.com",
        logger=DummyLogger(),
        timeout=1,
        retries=2,
        backoff=0.0,
    )
    assert response.status_code == 200
    assert calls["count"] == 2


def test_request_with_retry_on_exception(monkeypatch):
    calls = {"count": 0}

    def request(method, url, timeout, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise requests.RequestException("boom")
        return DummyResponse(200)

    session = SimpleNamespace(request=request)
    monkeypatch.setattr(http_utils.time, "sleep", lambda *_: None)

    response = http_utils.request_with_retry(
        session,
        "GET",
        "https://example.com",
        logger=DummyLogger(),
        timeout=1,
        retries=2,
        backoff=0.0,
    )
    assert response.status_code == 200
    assert calls["count"] == 2
