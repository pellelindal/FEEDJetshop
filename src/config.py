"""Configuration loading and validation."""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import List, Optional

from dotenv import load_dotenv


@dataclass(frozen=True)
class Config:
    feed_token_url: str
    feed_client_id: str
    feed_client_secret: str
    feed_export_url: str
    jetshop_soap_url: str
    jetshop_username: str
    jetshop_password: str
    jetshop_shop_id: str
    jetshop_soap_header_xml: Optional[str]
    jetshop_template_id: Optional[str]
    cultures: List[str]
    log_file: str
    mapping_file: str
    log_level: str
    http_timeout: float
    retry_count: int
    retry_backoff: float


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _parse_list(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def load_config() -> Config:
    load_dotenv()

    cultures = os.getenv("CULTURES", "sv-SE,nb-NO")
    log_file = os.getenv("LOG_FILE", "logs/integration.log")
    mapping_file = os.getenv("MAPPING_FILE", "mappings/mapping.yaml")

    return Config(
        feed_token_url=_require_env("FEED_TOKEN_URL"),
        feed_client_id=_require_env("FEED_CLIENT_ID"),
        feed_client_secret=_require_env("FEED_CLIENT_SECRET"),
        feed_export_url=_require_env("FEED_EXPORT_URL"),
        jetshop_soap_url=_require_env("JETSHOP_SOAP_URL"),
        jetshop_username=_require_env("JETSHOP_USERNAME"),
        jetshop_password=_require_env("JETSHOP_PASSWORD"),
        jetshop_shop_id=_require_env("JETSHOP_SHOP_ID"),
        jetshop_soap_header_xml=os.getenv("JETSHOP_SOAP_HEADER_XML"),
        jetshop_template_id=_normalize_optional(os.getenv("JETSHOP_TEMPLATE_ID", "1")),
        cultures=_parse_list(cultures),
        log_file=log_file,
        mapping_file=mapping_file,
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        http_timeout=float(os.getenv("HTTP_TIMEOUT", "30")),
        retry_count=int(os.getenv("RETRY_COUNT", "3")),
        retry_backoff=float(os.getenv("RETRY_BACKOFF", "0.5")),
    )


def _normalize_optional(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned if cleaned else None
