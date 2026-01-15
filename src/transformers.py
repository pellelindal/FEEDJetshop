"""Transform pipeline for mapping values."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Callable, Dict, List, Optional

from .validator import is_empty


@dataclass(frozen=True)
class TransformContext:
    culture: Optional[str]
    feed_language: Optional[str]
    fallback_language: Optional[str]
    attribute: Optional[Dict[str, Any]]


def newline_to_br(value: Any, _context: TransformContext, **_kwargs: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return value.replace("\r\n", "\n").replace("\n", "<br>")
    return value


def format_price(value: Any, _context: TransformContext, **_kwargs: Any) -> Any:
    if value is None:
        return None
    try:
        dec = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return value
    quantized = dec.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    return f"{quantized:.4f}"


def join_list(value: Any, _context: TransformContext, join_delimiter: str = ", ", **_kwargs: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, list):
        return join_delimiter.join([str(item) for item in value])
    return value


def data_register_label(
    value: Any,
    context: TransformContext,
    join_delimiter: str = ", ",
    **_kwargs: Any,
) -> Any:
    if value is None:
        return None

    attribute = None
    raw_value = value
    if isinstance(value, dict) and "options" in value and "value" in value:
        attribute = value
        raw_value = value.get("value")
    elif context.attribute:
        attribute = context.attribute

    if not attribute:
        return value

    options = attribute.get("options") or {}
    feed_lang = context.feed_language
    fallback_lang = context.fallback_language

    def map_code(code: Any) -> Any:
        code_str = str(code)
        labels = options.get(code_str) or {}
        if isinstance(labels, dict):
            if feed_lang:
                label = labels.get(feed_lang)
                if not is_empty(label):
                    return label
            if fallback_lang:
                label = labels.get(fallback_lang)
                if not is_empty(label):
                    return label
        return code_str

    if isinstance(raw_value, list):
        mapped = [map_code(code) for code in raw_value]
        return join_delimiter.join([str(item) for item in mapped])

    return map_code(raw_value)


TRANSFORM_REGISTRY: Dict[str, Callable[..., Any]] = {
    "newline_to_br": newline_to_br,
    "format_price": format_price,
    "join_list": join_list,
    "data_register_label": data_register_label,
}


def apply_transforms(value: Any, transforms: List[Any], context: TransformContext) -> Any:
    current = value
    for spec in transforms:
        transform = TRANSFORM_REGISTRY[spec.name]
        current = transform(current, context, **spec.args)
    return current
