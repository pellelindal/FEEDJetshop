import json
import logging
from datetime import datetime, timezone

from src.logging_setup import JsonFormatter, MergeExtraAdapter, TruncatingFileHandler


def test_json_formatter_includes_extras():
    formatter = JsonFormatter()
    record = logging.LogRecord("test", logging.INFO, __file__, 10, "hello", args=(), exc_info=None)
    record.runId = "run-123"
    record.productNo = "Pelle-1092-10"

    payload = json.loads(formatter.format(record))
    assert payload["message"] == "hello"
    assert payload["runId"] == "run-123"
    assert payload["productNo"] == "Pelle-1092-10"


def test_merge_extra_adapter_merges_extras():
    logger = logging.getLogger("test_merge_extra")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)

    class CaptureHandler(logging.Handler):
        def __init__(self):
            super().__init__()
            self.records = []

        def emit(self, record):
            self.records.append(record)

    handler = CaptureHandler()
    logger.addHandler(handler)

    adapter = MergeExtraAdapter(logger, {"runId": "run-1"})
    adapter.info("hello", extra={"event": "test"})

    assert handler.records
    payload = json.loads(JsonFormatter().format(handler.records[0]))
    assert payload["runId"] == "run-1"
    assert payload["event"] == "test"


def test_json_formatter_serializes_datetime():
    formatter = JsonFormatter()
    record = logging.LogRecord("test", logging.INFO, __file__, 10, "hello", args=(), exc_info=None)
    record.startTime = datetime(2026, 1, 16, 7, 30, 0, tzinfo=timezone.utc)

    payload = json.loads(formatter.format(record))
    assert payload["startTime"] == "2026-01-16T07:30:00+00:00"


def test_truncating_file_handler_limits_size(tmp_path):
    log_path = tmp_path / "test.log"
    handler = TruncatingFileHandler(log_path, max_bytes=400)
    handler.setFormatter(JsonFormatter())

    logger = logging.getLogger("test_truncate")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    for _ in range(100):
        logger.info("x" * 50)

    handler.flush()
    assert log_path.stat().st_size <= 400
