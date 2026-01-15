import os

import pytest
from dotenv import load_dotenv

from src.config import load_config
from src.jetshop_client import JetshopClient
from src.logging_setup import setup_logging


def _integration_enabled() -> bool:
    return os.getenv("RUN_INTEGRATION_TESTS") == "1" and os.getenv("RUN_JETSHOP_TESTS") == "1"


def _env_ready() -> bool:
    required = [
        "FEED_TOKEN_URL",
        "FEED_CLIENT_ID",
        "FEED_CLIENT_SECRET",
        "FEED_EXPORT_URL",
        "JETSHOP_SOAP_URL",
        "JETSHOP_USERNAME",
        "JETSHOP_PASSWORD",
        "JETSHOP_SHOP_ID",
    ]
    return all(os.getenv(name) for name in required)


@pytest.mark.integration
def test_live_jetshop_product_get():
    load_dotenv()
    if not _integration_enabled():
        pytest.skip("Set RUN_INTEGRATION_TESTS=1 and RUN_JETSHOP_TESTS=1")
    if not _env_ready():
        pytest.skip("Missing FEED/JETSHOP credentials in environment")
    config = load_config()
    logger = setup_logging(config.log_file, config.log_level, "integration-jetshop")
    client = JetshopClient(config, logger)

    product = client.product_get("sv-SE", "Pelle-1092-10")
    assert product is not None
