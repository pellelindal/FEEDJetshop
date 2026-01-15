"""Mapping YAML loader and validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

import yaml

from .transformers import TRANSFORM_REGISTRY


class MappingError(Exception):
    pass


@dataclass(frozen=True)
class TransformSpec:
    name: str
    args: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FieldMapping:
    target: str
    source: Optional[str]
    source_by_culture: Optional[Dict[str, str]]
    fallback_by_culture: Optional[Dict[str, str]]
    cultures: Optional[List[str]]
    fallback: Optional[str]
    type: str
    item_type: Optional[str]
    coerce: str
    transforms: List[TransformSpec]
    validations: Dict[str, Any]
    optional: bool
    preserve_if_missing: bool
    allow_empty: bool


@dataclass(frozen=True)
class DynamicFieldMapping:
    key: str
    source: Optional[str]
    source_by_culture: Optional[Dict[str, str]]
    fallback_by_culture: Optional[Dict[str, str]]
    cultures: Optional[List[str]]
    fallback: Optional[str]
    type: str
    item_type: Optional[str]
    coerce: str
    transforms: List[TransformSpec]
    validations: Dict[str, Any]
    optional: bool
    allow_empty: bool


@dataclass(frozen=True)
class CategoryMapping:
    source: str
    type: str
    item_type: Optional[str]
    coerce: str
    strategy: str
    optional: bool


@dataclass(frozen=True)
class PriceListMapping:
    name: Optional[str]
    price_list_id: str
    price_source: str
    discounted_price_source: Optional[str]
    discount_period_source: Optional[str]
    hide_product_source: Optional[str]
    clear_discount_on_missing: bool
    type: str
    coerce: str
    optional: bool


@dataclass(frozen=True)
class AutoDynamicFieldConfig:
    enabled: bool
    coerce: str
    type: str
    include_data_types: Optional[List[str]]
    join_delimiter: str
    skip_range: bool
    allowed_keys: List[str]


@dataclass(frozen=True)
class MappingConfig:
    version: int
    cultures: List[str]
    fallbacks: Dict[str, str]
    culture_map: Dict[str, str]
    product_fields: List[FieldMapping]
    stock_fields: List[FieldMapping]
    category_fields: CategoryMapping
    dynamic_fields_auto_map: AutoDynamicFieldConfig
    dynamic_fields_allowlist: List[DynamicFieldMapping]
    price_lists: List[PriceListMapping]

    def mapped_attribute_codes(self) -> List[str]:
        sources = _collect_sources(self)
        return sorted(sources["attributes"])

    def mapped_text_codes(self) -> List[str]:
        sources = _collect_sources(self)
        return sorted(sources["texts"])

    def dynamic_field_keys(self) -> List[str]:
        return sorted({entry.key for entry in self.dynamic_fields_allowlist})


def load_mapping(path: str | Path) -> MappingConfig:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise MappingError("Mapping root must be a dictionary")

    version = raw.get("version")
    if not isinstance(version, int):
        raise MappingError("Mapping version must be an integer")

    cultures = raw.get("cultures")
    if not isinstance(cultures, list) or not cultures:
        raise MappingError("Mapping cultures must be a non-empty list")

    fallbacks = raw.get("fallbacks") or {}
    if not isinstance(fallbacks, dict):
        raise MappingError("Mapping fallbacks must be a dictionary")

    culture_map = raw.get("culture_map") or {"sv-SE": "sv", "nb-NO": "nb"}
    if not isinstance(culture_map, dict):
        raise MappingError("Mapping culture_map must be a dictionary")

    product_fields = _parse_field_mappings(raw.get("product_fields"), "product_fields")
    stock_fields = _parse_field_mappings(raw.get("stock_fields"), "stock_fields")

    category_fields_raw = raw.get("category_fields")
    if not isinstance(category_fields_raw, dict):
        raise MappingError("category_fields must be a mapping object")
    category_fields = _parse_category_mapping(category_fields_raw)

    dynamic_fields = _parse_dynamic_mappings(raw.get("dynamic_fields_allowlist"))

    return MappingConfig(
        version=version,
        cultures=cultures,
        fallbacks=fallbacks,
        culture_map=culture_map,
        product_fields=product_fields,
        stock_fields=stock_fields,
        category_fields=category_fields,
        dynamic_fields_auto_map=_parse_auto_dynamic_fields(raw.get("dynamic_fields_auto_map")),
        dynamic_fields_allowlist=dynamic_fields,
        price_lists=_parse_price_lists(raw.get("price_lists")),
    )


def _parse_field_mappings(items: Any, name: str) -> List[FieldMapping]:
    if not isinstance(items, list) or not items:
        raise MappingError(f"{name} must be a non-empty list")
    mappings = []
    for item in items:
        parsed = _parse_mapping_entry(item, require_target=True)
        parsed.pop("key", None)
        mappings.append(FieldMapping(**parsed))
    return mappings


def _parse_dynamic_mappings(items: Any) -> List[DynamicFieldMapping]:
    if not isinstance(items, list):
        raise MappingError("dynamic_fields_allowlist must be a list")
    mappings = []
    for item in items:
        parsed = _parse_mapping_entry(item, require_key=True)
        parsed.pop("target", None)
        parsed.pop("preserve_if_missing", None)
        mappings.append(DynamicFieldMapping(**parsed))
    return mappings


def _parse_category_mapping(item: Dict[str, Any]) -> CategoryMapping:
    source = item.get("source")
    if not source:
        raise MappingError("category_fields.source is required")
    strategy = item.get("strategy", "replace")
    if strategy != "replace":
        raise MappingError("category_fields.strategy must be 'replace'")
    coerce = item.get("coerce", "strict")
    return CategoryMapping(
        source=source,
        type=item.get("type", "list"),
        item_type=item.get("item_type"),
        coerce=coerce,
        strategy=strategy,
        optional=bool(item.get("optional", False)),
    )


def _parse_price_lists(items: Any) -> List[PriceListMapping]:
    if items is None:
        return []
    if not isinstance(items, list):
        raise MappingError("price_lists must be a list")
    mappings: List[PriceListMapping] = []
    for item in items:
        if not isinstance(item, dict):
            raise MappingError("price_lists entries must be dictionaries")
        price_list_id = item.get("price_list_id")
        price_source = item.get("price_source")
        if not price_list_id or not price_source:
            raise MappingError("price_lists entries require price_list_id and price_source")
        coerce = item.get("coerce", "strict")
        if coerce not in {"strict", "coerce"}:
            raise MappingError("price_lists.coerce must be 'strict' or 'coerce'")
        mappings.append(
            PriceListMapping(
                name=item.get("name"),
                price_list_id=price_list_id,
                price_source=price_source,
                discounted_price_source=item.get("discounted_price_source"),
                discount_period_source=item.get("discount_period_source"),
                hide_product_source=item.get("hide_product_source"),
                clear_discount_on_missing=bool(item.get("clear_discount_on_missing", False)),
                type=item.get("type", "int"),
                coerce=coerce,
                optional=bool(item.get("optional", False)),
            )
        )
    return mappings


def _parse_auto_dynamic_fields(value: Any) -> AutoDynamicFieldConfig:
    if value is None:
        return AutoDynamicFieldConfig(
            enabled=False,
            coerce="coerce",
            type="string",
            include_data_types=None,
            join_delimiter=", ",
            skip_range=True,
            allowed_keys=[],
        )

    if isinstance(value, bool):
        return AutoDynamicFieldConfig(
            enabled=value,
            coerce="coerce",
            type="string",
            include_data_types=None,
            join_delimiter=", ",
            skip_range=True,
            allowed_keys=[],
        )

    if not isinstance(value, dict):
        raise MappingError("dynamic_fields_auto_map must be a boolean or mapping object")

    coerce = value.get("coerce", "coerce")
    if coerce not in {"strict", "coerce"}:
        raise MappingError("dynamic_fields_auto_map.coerce must be 'strict' or 'coerce'")

    field_type = value.get("type", "string")
    allowed_types = {"string", "int", "float", "decimal", "bool", "date", "datetime", "list"}
    if field_type not in allowed_types:
        raise MappingError("dynamic_fields_auto_map.type must be a valid field type")

    include_data_types = value.get("include_data_types")
    if include_data_types is not None:
        if not isinstance(include_data_types, list) or not all(
            isinstance(item, str) for item in include_data_types
        ):
            raise MappingError("dynamic_fields_auto_map.include_data_types must be a list of strings")

    join_delimiter = value.get("join_delimiter", ", ")
    if not isinstance(join_delimiter, str):
        raise MappingError("dynamic_fields_auto_map.join_delimiter must be a string")

    allowed_keys = value.get("allowed_keys") or []
    if not isinstance(allowed_keys, list) or not all(isinstance(item, str) for item in allowed_keys):
        raise MappingError("dynamic_fields_auto_map.allowed_keys must be a list of strings")

    return AutoDynamicFieldConfig(
        enabled=bool(value.get("enabled", False)),
        coerce=coerce,
        type=field_type,
        include_data_types=include_data_types,
        join_delimiter=join_delimiter,
        skip_range=bool(value.get("skip_range", True)),
        allowed_keys=allowed_keys,
    )


def _parse_mapping_entry(item: Any, require_target: bool = False, require_key: bool = False) -> Dict[str, Any]:
    if not isinstance(item, dict):
        raise MappingError("Mapping entries must be dictionaries")

    if require_target and "target" not in item:
        raise MappingError("Mapping entry is missing target")
    if require_key and "key" not in item:
        raise MappingError("Dynamic field mapping entry is missing key")

    source = item.get("source")
    source_by_culture = item.get("source_by_culture")
    if not source and not source_by_culture:
        raise MappingError("Mapping entry must include source or source_by_culture")

    coerce = item.get("coerce", "strict")
    if coerce not in {"strict", "coerce"}:
        raise MappingError("coerce must be 'strict' or 'coerce'")

    transforms = _parse_transforms(item.get("transforms") or [])

    return {
        "target": item.get("target"),
        "key": item.get("key"),
        "source": source,
        "source_by_culture": source_by_culture,
        "fallback_by_culture": item.get("fallback_by_culture"),
        "cultures": item.get("cultures"),
        "fallback": item.get("fallback"),
        "type": item.get("type", "string"),
        "item_type": item.get("item_type"),
        "coerce": coerce,
        "transforms": transforms,
        "validations": item.get("validations") or {},
        "optional": bool(item.get("optional", False)),
        "preserve_if_missing": bool(item.get("preserve_if_missing", False)),
        "allow_empty": bool(item.get("allow_empty", False)),
    }


def _parse_transforms(items: Sequence[Any]) -> List[TransformSpec]:
    transforms: List[TransformSpec] = []
    for item in items:
        if isinstance(item, str):
            name = item
            args: Dict[str, Any] = {}
        elif isinstance(item, dict):
            name = item.get("name")
            args = item.get("args") or {}
        else:
            raise MappingError("Transform entries must be strings or objects")
        if name not in TRANSFORM_REGISTRY:
            raise MappingError(f"Unknown transform: {name}")
        transforms.append(TransformSpec(name=name, args=args))
    return transforms


_SOURCE_RE = re.compile(r"^(?P<root>texts|attributes)\[(?P<key>[^\]]+)\](?:\.(?P<path>.+))?$")


def parse_source_selector(source: str) -> Tuple[str, Optional[str], List[str]]:
    match = _SOURCE_RE.match(source)
    if match:
        root = match.group("root")
        key = match.group("key")
        path_str = match.group("path") or ""
        path = [segment for segment in path_str.split(".") if segment]
        return root, key, path

    root, *rest = source.split(".")
    path = rest if rest else []
    return root, None, path


def _collect_sources(mapping: MappingConfig) -> Dict[str, set]:
    result = {"texts": set(), "attributes": set()}

    def add_source(source: Optional[str]) -> None:
        if not source:
            return
        root, key, _path = parse_source_selector(source)
        if root in result and key:
            result[root].add(key)

    for entry in mapping.product_fields + mapping.stock_fields:
        add_source(entry.source)
        if entry.source_by_culture:
            for src in entry.source_by_culture.values():
                add_source(src)

    add_source(mapping.category_fields.source)

    for entry in mapping.dynamic_fields_allowlist:
        add_source(entry.source)
        if entry.source_by_culture:
            for src in entry.source_by_culture.values():
                add_source(src)

    for entry in mapping.price_lists:
        add_source(entry.price_source)
        add_source(entry.discounted_price_source)
        add_source(entry.discount_period_source)
        add_source(entry.hide_product_source)

    return result
