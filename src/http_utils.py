"""HTTP retry helpers with exponential backoff."""

from __future__ import annotations

import time
from typing import Iterable, Optional

import requests


RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    logger,
    timeout: float,
    retries: int,
    backoff: float,
    retryable_status: Optional[Iterable[int]] = None,
    **kwargs,
) -> requests.Response:
    retryable = set(retryable_status or RETRYABLE_STATUS)
    attempt = 0
    while True:
        try:
            response = session.request(method, url, timeout=timeout, **kwargs)
            if response.status_code in retryable and attempt < retries:
                _sleep(backoff, attempt)
                attempt += 1
                continue
            return response
        except requests.RequestException as exc:
            if attempt >= retries:
                raise
            logger.warning(
                "http_retry",
                extra={
                    "event": "http_retry",
                    "detail": str(exc),
                    "attempt": attempt + 1,
                    "url": url,
                },
            )
            _sleep(backoff, attempt)
            attempt += 1


def _sleep(backoff: float, attempt: int) -> None:
    delay = backoff * (2**attempt)
    time.sleep(delay)
