import os

import pytest
from dotenv import load_dotenv

from src.config import Config
from src.feed_client import FeedClient
from src.logging_setup import setup_logging


def _integration_enabled() -> bool:
    return os.getenv("RUN_INTEGRATION_TESTS") == "1"


def _feed_env_ready() -> bool:
    required = ["FEED_TOKEN_URL", "FEED_CLIENT_ID", "FEED_CLIENT_SECRET", "FEED_EXPORT_URL"]
    return all(os.getenv(name) for name in required)


@pytest.mark.integration
def test_live_feed_fetch_single_product():
    load_dotenv()
    if not _integration_enabled():
        pytest.skip("Set RUN_INTEGRATION_TESTS=1 to enable")
    if not _feed_env_ready():
        pytest.skip("Missing FEED credentials in environment")

    config = Config(
        feed_token_url=os.getenv("FEED_TOKEN_URL"),
        feed_client_id=os.getenv("FEED_CLIENT_ID"),
        feed_client_secret=os.getenv("FEED_CLIENT_SECRET"),
        feed_export_url=os.getenv("FEED_EXPORT_URL"),
        jetshop_soap_url="https://example.invalid",
        jetshop_username="user",
        jetshop_password="pass",
        jetshop_shop_id="shop1",
        jetshop_soap_header_xml=None,
        jetshop_template_id=os.getenv("JETSHOP_TEMPLATE_ID", "1"),
        cultures=["sv-SE", "nb-NO"],
        log_file="logs/integration_test.log",
        mapping_file="mappings/mapping.yaml",
        log_level="INFO",
        http_timeout=30,
        retry_count=1,
        retry_backoff=0.2,
    )

    logger = setup_logging(config.log_file, config.log_level, "integration-feed")
    client = FeedClient(config, logger)
    products = client.fetch_products(
        "2025-01-01T00:00:00Z",
        product_no="Pelle-1092-10",
        limit=1,
    )
    assert products
    assert products[0]["identifier"]["productNo"] == "Pelle-1092-10"
