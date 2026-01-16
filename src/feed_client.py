"""FEED API client with OAuth token caching."""

from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any, Dict, List, Optional

import requests

from .config import Config
from .http_utils import request_with_retry


@dataclass
class FeedToken:
    access_token: str
    expires_at: float


class FeedClient:
    def __init__(self, config: Config, logger) -> None:
        self.config = config
        self.logger = logger
        self.session = requests.Session()
        self._token: Optional[FeedToken] = None

    def get_token(self) -> str:
        if self._token and time.time() < self._token.expires_at - 60:
            return self._token.access_token

        start = time.monotonic()
        success = False
        error_message = None
        data = {
            "grant_type": "client_credentials",
            "client_id": self.config.feed_client_id,
            "client_secret": self.config.feed_client_secret,
        }
        try:
            response = request_with_retry(
                self.session,
                "POST",
                self.config.feed_token_url,
                logger=self.logger,
                timeout=self.config.http_timeout,
                retries=self.config.retry_count,
                backoff=self.config.retry_backoff,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            response.raise_for_status()
            payload = response.json()
            access_token = payload.get("access_token")
            expires_in = int(payload.get("expires_in", 3600))
            if not access_token:
                raise ValueError("FEED token response missing access_token")
            self._token = FeedToken(access_token=access_token, expires_at=time.time() + expires_in)
            success = True
            return access_token
        except Exception as exc:
            error_message = str(exc)
            raise
        finally:
            duration_ms = int((time.monotonic() - start) * 1000)
            log_fn = self.logger.info if success else self.logger.error
            extra = {
                "event": "feed_token",
                "durationMs": duration_ms,
                "success": success,
            }
            if error_message:
                extra["detail"] = error_message
            log_fn("feed_token", extra=extra)

    def fetch_products(
        self,
        export_from: str,
        product_no: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        start = time.monotonic()
        success = False
        error_message = None
        token = self.get_token()
        export_url = self.config.feed_export_url.rstrip("/")
        if export_url.endswith("/full"):
            export_url = export_url[: -len("/full")]

        page_size = limit if limit is not None else 20
        params = {
            "showInactive": "true",
            "orderByLanguageCode": "nb",
            "dateFormat": "SHORT",
            "page": 0,
            "size": page_size,
            "exportFrom": export_from,
            "changesOnly": "true",
            "includeDeleted": "true",
            "includeModifiedByBasedata": "true",
            "productHeadOnly": "false",
            "includeOptions": "true",
            "includeLastModifiedTimestamp": "false",
        }
        if product_no:
            params["productNo"] = product_no

        try:
            response = request_with_retry(
                self.session,
                "POST",
                export_url,
                logger=self.logger,
                timeout=self.config.http_timeout,
                retries=self.config.retry_count,
                backoff=self.config.retry_backoff,
                params=params,
                headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            )
            response.raise_for_status()
            payload = response.json()
            content = payload.get("content") or []
            total_pages = payload.get("totalPages", 1)
            if total_pages and total_pages > 1:
                self.logger.warning(
                    "feed_paged_response",
                    extra={"event": "feed_paged_response", "totalPages": total_pages},
                )
            if limit:
                content = content[:limit]
            success = True
            return content
        except Exception as exc:
            error_message = str(exc)
            raise
        finally:
            duration_ms = int((time.monotonic() - start) * 1000)
            log_fn = self.logger.info if success else self.logger.error
            extra = {
                "event": "feed_fetch",
                "durationMs": duration_ms,
                "productNo": product_no,
                "success": success,
            }
            if error_message:
                extra["detail"] = error_message
            log_fn("feed_fetch", extra=extra)
