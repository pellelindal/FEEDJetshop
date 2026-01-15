import os

from src.config import load_config


def test_load_config_from_env(monkeypatch):
    monkeypatch.setenv("FEED_TOKEN_URL", "https://example.com/token")
    monkeypatch.setenv("FEED_CLIENT_ID", "client")
    monkeypatch.setenv("FEED_CLIENT_SECRET", "secret")
    monkeypatch.setenv("FEED_EXPORT_URL", "https://example.com/export")
    monkeypatch.setenv("JETSHOP_SOAP_URL", "https://example.com/soap")
    monkeypatch.setenv("JETSHOP_USERNAME", "user")
    monkeypatch.setenv("JETSHOP_PASSWORD", "pass")
    monkeypatch.setenv("JETSHOP_SHOP_ID", "shop1")
    monkeypatch.setenv("JETSHOP_TEMPLATE_ID", "1")
    monkeypatch.setenv("CULTURES", "sv-SE,nb-NO")

    config = load_config()
    assert config.feed_token_url == "https://example.com/token"
    assert config.feed_client_id == "client"
    assert config.jetshop_shop_id == "shop1"
    assert config.jetshop_template_id == "1"
    assert config.cultures == ["sv-SE", "nb-NO"]
