"""Mapping discovery to highlight unmapped fields."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from .mapping_loader import MappingConfig


DATA_TYPE_SUGGESTION = {
    "FLOAT": "float",
    "INT": "int",
    "UNI_TEXT": "string",
    "TEXT": "string",
    "DATA_REGISTER": "string",
    "DATA_REGISTER_MULTI": "list",
}


def discover_mapping(
    feed_client,
    jetshop_client,
    mapping: MappingConfig,
    export_from: str,
    product_no: Optional[str],
    output_path: str = "mappings/mapping_suggestions.yaml",
) -> Dict[str, Any]:
    products = feed_client.fetch_products(export_from, product_no, limit=1)
    if not products:
        raise ValueError("No FEED products returned for discovery")

    product = products[0]
    mapped_attrs = set(mapping.mapped_attribute_codes())
    mapped_texts = set(mapping.mapped_text_codes())
    mapped_dynamic = set(mapping.dynamic_field_keys())

    suggestions: Dict[str, Any] = {
        "unmapped_attributes": [],
        "unmapped_texts": [],
        "unmapped_dynamic_fields": [],
    }

    for attr in product.get("attributes", []):
        import_code = attr.get("importCode")
        if not import_code or import_code in mapped_attrs:
            continue
        value = attr.get("value")
        cultures = sorted(list(value.keys())) if isinstance(value, dict) else []
        transforms = []
        if attr.get("dataType") in {"DATA_REGISTER", "DATA_REGISTER_MULTI"}:
            transforms.append("data_register_label")
        suggestions["unmapped_attributes"].append(
            {
                "importCode": import_code,
                "dataType": attr.get("dataType"),
                "sampleValue": value,
                "culturesPresent": cultures,
                "recommendedTransforms": transforms,
                "suggestedTargetType": DATA_TYPE_SUGGESTION.get(attr.get("dataType"), "string"),
            }
        )

    for text in product.get("texts", []):
        import_code = text.get("importCode")
        if not import_code or import_code in mapped_texts:
            continue
        value = text.get("value")
        cultures = sorted(list(value.keys())) if isinstance(value, dict) else []
        transforms = []
        if isinstance(value, dict):
            if any("\n" in str(item) for item in value.values()):
                transforms.append("newline_to_br")
        suggestions["unmapped_texts"].append(
            {
                "importCode": import_code,
                "maxLength": text.get("maxLength"),
                "sampleValue": value,
                "culturesPresent": cultures,
                "recommendedTransforms": transforms,
                "suggestedTargetType": "string",
            }
        )

    product_no_value = product_no or (product.get("identifier") or {}).get("productNo")
    if product_no_value:
        dynamic_fields = jetshop_client.dyn_get([product_no_value], mapping.cultures)
        for key, values in dynamic_fields.items():
            if key in mapped_dynamic:
                continue
            suggestions["unmapped_dynamic_fields"].append(
                {
                    "key": key,
                    "sampleValues": values,
                    "suggestedTargetType": "string",
                }
            )

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text(
        yaml.safe_dump(suggestions, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    return suggestions
