"""Diff computation for product and dynamic fields."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class DiffItem:
    target_field: str
    old_value: Any
    new_value: Any
    culture: Optional[str] = None
    section: Optional[str] = None


def _normalize(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return f"{value:.4f}"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return Decimal(str(value))
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def diff_product_data(
    current: Dict[str, Any],
    desired: Dict[str, Any],
    culture: Optional[str],
) -> List[DiffItem]:
    diffs: List[DiffItem] = []
    for key, desired_value in desired.items():
        if key in {"ProductInCategories", "StockData"}:
            continue
        current_value = current.get(key)
        if _normalize(current_value) != _normalize(desired_value):
            diffs.append(DiffItem(key, current_value, desired_value, culture=culture, section="ProductData"))
    return diffs


def diff_categories(
    current_categories: List[str],
    desired_categories: List[str],
    culture: Optional[str],
) -> List[DiffItem]:
    if sorted(current_categories) != sorted(desired_categories):
        return [
            DiffItem(
                "ProductInCategories",
                current_categories,
                desired_categories,
                culture=culture,
                section="Categories",
            )
        ]
    return []


def diff_stock(
    current_stock: Dict[str, Any],
    desired_stock: Dict[str, Any],
    culture: Optional[str],
) -> List[DiffItem]:
    diffs: List[DiffItem] = []
    for key, desired_value in desired_stock.items():
        current_value = current_stock.get(key)
        if _normalize(current_value) != _normalize(desired_value):
            diffs.append(DiffItem(key, current_value, desired_value, culture=culture, section="StockData"))
    return diffs


def diff_dynamic_fields(
    current: Dict[str, Dict[str, Any]],
    desired: Dict[str, Dict[str, Any]],
) -> List[DiffItem]:
    diffs: List[DiffItem] = []
    for key, cultures in desired.items():
        for culture, desired_value in cultures.items():
            current_value = current.get(key, {}).get(culture)
            if _normalize(current_value) != _normalize(desired_value):
                diffs.append(
                    DiffItem(
                        target_field=key,
                        old_value=current_value,
                        new_value=desired_value,
                        culture=culture,
                        section="DynamicFields",
                    )
                )
    return diffs
