"""Structured JSON logging to console and rotating file."""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict


STANDARD_ATTRS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
        }

        extras = {
            key: value for key, value in record.__dict__.items() if key not in STANDARD_ATTRS
        }
        payload.update(extras)

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=True, default=_json_default)


class TruncatingFileHandler(logging.FileHandler):
    """File handler that keeps the newest log content within a size limit."""

    def __init__(self, filename: str | Path, max_bytes: int) -> None:
        super().__init__(filename, mode="a", encoding="utf-8", delay=False)
        self.max_bytes = max_bytes

    def emit(self, record: logging.LogRecord) -> None:
        super().emit(record)
        try:
            self._truncate_if_needed()
        except Exception:
            self.handleError(record)

    def _truncate_if_needed(self) -> None:
        if self.max_bytes <= 0:
            return

        try:
            self.stream.flush()
        except Exception:
            return

        path = self.baseFilename
        try:
            size = os.path.getsize(path)
        except OSError:
            return
        if size <= self.max_bytes:
            return

        try:
            with open(path, "rb+") as handle:
                handle.seek(0, os.SEEK_END)
                size = handle.tell()
                if size <= self.max_bytes:
                    return
                start = max(0, size - self.max_bytes)
                handle.seek(start)
                data = handle.read()
                newline_index = data.find(b"\n")
                if newline_index != -1:
                    data = data[newline_index + 1 :]
                handle.seek(0)
                handle.write(data)
                handle.truncate()
        except OSError:
            return


def _json_default(value: Any) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


class MergeExtraAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        extra = kwargs.get("extra", {})
        merged = dict(self.extra)
        merged.update(extra)
        kwargs["extra"] = merged
        return msg, kwargs


def setup_logging(log_file: str, level: str, run_id: str) -> logging.LoggerAdapter:
    logger = logging.getLogger("feed_jetshop")
    logger.setLevel(level)
    logger.handlers.clear()
    logger.propagate = False

    formatter = JsonFormatter()

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level)

    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = TruncatingFileHandler(log_path, max_bytes=5 * 1024 * 1024)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return MergeExtraAdapter(logger, {"runId": run_id})
