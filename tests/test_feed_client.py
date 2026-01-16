import json
import logging

from src.config import Config
from src.feed_client import FeedClient


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload
        self.status_code = 200
        self.text = json.dumps(payload, ensure_ascii=True)

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def _build_client():
    config = Config(
        feed_token_url="https://example.invalid/token",
        feed_client_id="client",
        feed_client_secret="secret",
        feed_export_url="https://example.invalid/export/export/full",
        jetshop_soap_url="https://example.invalid/soap",
        jetshop_username="user",
        jetshop_password="pass",
        jetshop_shop_id="shop1",
        jetshop_soap_header_xml=None,
        jetshop_template_id="1",
        cultures=["sv-SE", "nb-NO"],
        log_file="logs/test.log",
        mapping_file="mappings/mapping.yaml",
        log_level="INFO",
        http_timeout=5,
        retry_count=1,
        retry_backoff=0.1,
    )
    logger = logging.getLogger("test_feed_client")
    logger.addHandler(logging.NullHandler())
    client = FeedClient(config, logger)
    client.get_token = lambda: "token"
    return client


def test_fetch_products_paginates(monkeypatch):
    client = _build_client()
    calls = []

    payloads = [
        {
            "content": [{"identifier": {"productNo": "A"}}],
            "totalPages": 2,
            "last": False,
            "number": 0,
            "numberOfElements": 1,
            "pageable": {"paged": True, "unpaged": False},
        },
        {
            "content": [{"identifier": {"productNo": "B"}}],
            "totalPages": 2,
            "last": True,
            "number": 1,
            "numberOfElements": 1,
            "pageable": {"paged": True, "unpaged": False},
        },
    ]

    def fake_request_with_retry(session, method, url, **kwargs):
        calls.append(kwargs.get("params", {}))
        return FakeResponse(payloads[len(calls) - 1])

    monkeypatch.setattr("src.feed_client.request_with_retry", fake_request_with_retry)

    products = client.fetch_products("2025-01-01T00:00:00Z")

    assert [item["identifier"]["productNo"] for item in products] == ["A", "B"]
    assert calls[0]["page"] == 0
    assert calls[1]["page"] == 1


def test_fetch_products_limit_stops_early(monkeypatch):
    client = _build_client()
    calls = []

    payloads = [
        {
            "content": [{"identifier": {"productNo": "A"}}],
            "totalPages": 2,
            "last": False,
            "number": 0,
            "numberOfElements": 1,
            "pageable": {"paged": True, "unpaged": False},
        },
        {
            "content": [{"identifier": {"productNo": "B"}}],
            "totalPages": 2,
            "last": True,
            "number": 1,
            "numberOfElements": 1,
            "pageable": {"paged": True, "unpaged": False},
        },
    ]

    def fake_request_with_retry(session, method, url, **kwargs):
        calls.append(kwargs.get("params", {}))
        return FakeResponse(payloads[len(calls) - 1])

    monkeypatch.setattr("src.feed_client.request_with_retry", fake_request_with_retry)

    products = client.fetch_products("2025-01-01T00:00:00Z", limit=1)

    assert [item["identifier"]["productNo"] for item in products] == ["A"]
    assert len(calls) == 1
