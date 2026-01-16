"""FEED API client with OAuth token caching."""

from __future__ import annotations

from dataclasses import dataclass
import json
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

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
        self._base_url = _derive_base_url(config.feed_export_url)

    def get_token(self) -> str:
        if self._token and time.time() < self._token.expires_at - 60:
            return self._token.access_token

        start = time.monotonic()
        success = False
        error_message = None
        response_text = None
        status_code = None
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
            status_code = response.status_code
            response_text = response.text
            response.raise_for_status()
            payload = response.json()
            response_text = json.dumps(_redact_token_payload(payload), ensure_ascii=True)
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
            if response_text is not None:
                self._log_api_response(
                    "feed_api_response",
                    response_text,
                    status_code,
                    success,
                    api="token",
                )

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

        page_size = 20
        page = 0
        all_content: List[Dict[str, Any]] = []
        total_pages: Optional[int] = None
        last_status_code = None

        try:
            while True:
                params = {
                    "showInactive": "true",
                    "orderByLanguageCode": "nb",
                    "dateFormat": "SHORT",
                    "page": page,
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
                last_status_code = response.status_code
                response.raise_for_status()
                payload = response.json()
                content = payload.get("content") or []
                all_content.extend(content)
                self._log_api_response(
                    "feed_api_response",
                    json.dumps(payload, ensure_ascii=True),
                    last_status_code,
                    True,
                    api="export",
                    page=page,
                    productNo=product_no,
                )

                if limit and len(all_content) >= limit:
                    all_content = all_content[:limit]
                    break

                total_pages = payload.get("totalPages", total_pages)
                last_page = payload.get("last")
                pageable = payload.get("pageable") or {}
                if last_page is True:
                    break
                if total_pages is not None and page >= max(total_pages - 1, 0):
                    break
                if not pageable.get("paged", True) or pageable.get("unpaged"):
                    break
                if payload.get("numberOfElements", len(content)) == 0:
                    break

                page += 1

            if total_pages and total_pages > 1:
                self.logger.info(
                    "feed_paged_response",
                    extra={"event": "feed_paged_response", "totalPages": total_pages, "pagesFetched": page + 1},
                )
            success = True
            return all_content
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

    def fetch_media_base64(self, media_code: str) -> str:
        start = time.monotonic()
        success = False
        error_message = None
        response_text = None
        status_code = None
        token = self.get_token()
        url = f"{self._base_url}/media/export/base64/mediaCode"
        params = {"mediaCode": media_code}

        try:
            response = request_with_retry(
                self.session,
                "GET",
                url,
                logger=self.logger,
                timeout=self.config.http_timeout,
                retries=self.config.retry_count,
                backoff=self.config.retry_backoff,
                params=params,
                headers={"Authorization": f"Bearer {token}", "Accept": "text/plain"},
            )
            status_code = response.status_code
            response_text = response.text
            response.raise_for_status()
            success = True
            return response.text.strip()
        except Exception as exc:
            error_message = str(exc)
            raise
        finally:
            duration_ms = int((time.monotonic() - start) * 1000)
            log_fn = self.logger.info if success else self.logger.error
            extra = {
                "event": "feed_media_fetch",
                "durationMs": duration_ms,
                "mediaCode": media_code,
                "success": success,
            }
            if error_message:
                extra["detail"] = error_message
            log_fn("feed_media_fetch", extra=extra)
            if response_text is not None:
                self._log_api_response(
                    "feed_api_response",
                    response_text,
                    status_code,
                    success,
                    api="media_base64",
                    mediaCode=media_code,
                )

    def _log_api_response(
        self,
        event: str,
        response_text: str,
        status_code: Optional[int],
        success: bool,
        **extra: Any,
    ) -> None:
        body, truncated, length = _truncate_response(response_text)
        payload = {
            "event": event,
            "statusCode": status_code,
            "success": success,
            "responseBody": body,
            "responseLength": length,
            "responseTruncated": truncated,
        }
        payload.update(extra)
        log_fn = self.logger.info if success else self.logger.error
        log_fn(event, extra=payload)


def _derive_base_url(feed_export_url: str) -> str:
    parsed = urlparse(feed_export_url)
    if not parsed.scheme or not parsed.netloc:
        return feed_export_url.rstrip("/")
    return f"{parsed.scheme}://{parsed.netloc}"


def _truncate_response(response_text: str, max_chars: int = 4000) -> tuple[str, bool, int]:
    text = response_text or ""
    length = len(text)
    if max_chars <= 0 or length <= max_chars:
        return text, False, length
    return text[:max_chars], True, length


def _redact_token_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    redacted = dict(payload)
    for key in ["access_token", "refresh_token"]:
        if key in redacted and redacted[key]:
            token = str(redacted[key])
            redacted[key] = f"{token[:6]}...{len(token)}"
    return redacted
