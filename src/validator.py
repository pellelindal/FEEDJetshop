"""Type coercion and validation helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
import re
from typing import Any, Optional


@dataclass(frozen=True)
class ValidationError(Exception):
    field: str
    message: str

    def __str__(self) -> str:
        return f"{self.field}: {self.message}"


def is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    if isinstance(value, list) and not value:
        return True
    return False


def coerce_value(value: Any, expected_type: str, policy: str, item_type: Optional[str] = None) -> Any:
    if value is None:
        return None

    try:
        if expected_type == "string":
            if isinstance(value, str):
                return value
            if policy == "coerce":
                return str(value)
            raise ValidationError(expected_type, f"expected string, got {type(value).__name__}")

        if expected_type == "int":
            if isinstance(value, bool):
                raise ValidationError(expected_type, "bool is not int")
            if isinstance(value, int):
                return value
            if policy == "coerce":
                return int(float(value))
            raise ValidationError(expected_type, f"expected int, got {type(value).__name__}")

        if expected_type == "float":
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                return float(value)
            if policy == "coerce":
                return float(value)
            raise ValidationError(expected_type, f"expected float, got {type(value).__name__}")

        if expected_type == "decimal":
            if isinstance(value, Decimal):
                return value
            if policy == "coerce":
                return Decimal(str(value))
            raise ValidationError(expected_type, f"expected decimal, got {type(value).__name__}")

        if expected_type == "bool":
            if isinstance(value, bool):
                return value
            if policy == "coerce":
                if isinstance(value, str):
                    lowered = value.strip().lower()
                    if lowered in {"true", "1", "yes"}:
                        return True
                    if lowered in {"false", "0", "no"}:
                        return False
                if isinstance(value, (int, float)):
                    return bool(value)
            raise ValidationError(expected_type, f"expected bool, got {type(value).__name__}")

        if expected_type == "date":
            if isinstance(value, date) and not isinstance(value, datetime):
                return value
            if policy == "coerce":
                return _parse_date(value)
            raise ValidationError(expected_type, f"expected date, got {type(value).__name__}")

        if expected_type == "datetime":
            if isinstance(value, datetime):
                return value
            if policy == "coerce":
                return _parse_datetime(value)
            raise ValidationError(expected_type, f"expected datetime, got {type(value).__name__}")

        if expected_type == "list":
            if isinstance(value, list):
                if item_type:
                    return [coerce_value(item, item_type, policy) for item in value]
                return value
            if policy == "coerce":
                return [value]
            raise ValidationError(expected_type, f"expected list, got {type(value).__name__}")

    except (ValueError, InvalidOperation, ValidationError) as exc:
        if isinstance(exc, ValidationError):
            raise
        raise ValidationError(expected_type, str(exc)) from exc

    raise ValidationError(expected_type, f"unknown expected type: {expected_type}")


def validate_constraints(value: Any, validations: dict, field_name: str) -> None:
    if value is None:
        return

    max_length = validations.get("max_length")
    if max_length is not None and isinstance(value, str) and len(value) > int(max_length):
        raise ValidationError(field_name, f"max_length {max_length} exceeded")

    min_value = validations.get("min")
    if min_value is not None and isinstance(value, (int, float, Decimal)):
        if value < Decimal(str(min_value)):
            raise ValidationError(field_name, f"value below min {min_value}")

    max_value = validations.get("max")
    if max_value is not None and isinstance(value, (int, float, Decimal)):
        if value > Decimal(str(max_value)):
            raise ValidationError(field_name, f"value above max {max_value}")

    regex = validations.get("regex")
    if regex is not None and isinstance(value, str):
        if not re.match(regex, value):
            raise ValidationError(field_name, "regex validation failed")


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = value.replace("Z", "+00:00")
        return datetime.fromisoformat(text)
    raise ValueError("invalid datetime value")


def _parse_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = value.split("T")[0]
        return date.fromisoformat(text)
    raise ValueError("invalid date value")
