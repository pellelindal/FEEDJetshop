"""Persistence for last successful run timestamp."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Optional


@dataclass
class StateStore:
    path: Path

    def read_last_run(self) -> Optional[str]:
        if not self.path.exists():
            return None
        data = json.loads(self.path.read_text(encoding="utf-8"))
        return data.get("last_run")

    def write_last_run(self, iso_timestamp: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"last_run": iso_timestamp}
        self.path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")

    def write_now(self) -> str:
        now = datetime.now(timezone.utc).isoformat()
        self.write_last_run(now)
        return now
