"""Synchronization engine for FEED -> Jetshop."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .diff_engine import diff_categories, diff_dynamic_fields, diff_product_data, diff_stock
from .jetshop_client import NIL_VALUE
from .mapping_loader import (
    DynamicFieldMapping,
    FieldMapping,
    MappingConfig,
    parse_source_selector,
)
from .transformers import TransformContext, apply_transforms
from .validator import ValidationError, coerce_value, is_empty, validate_constraints


@dataclass
class ProductProcessResult:
    product_no: str
    action: str
    success: bool
    errors: List[str]
    changes: int
    dynamic_changes: int


class SyncEngine:
    def __init__(
        self,
        feed_client,
        jetshop_client,
        mapping: MappingConfig,
        logger,
        state_store,
    ) -> None:
        self.feed_client = feed_client
        self.jetshop_client = jetshop_client
        self.mapping = mapping
        self.logger = logger
        self.state_store = state_store

    def sync(
        self,
        export_from: str,
        product_no: Optional[str],
        limit: Optional[int],
        dry_run: bool,
    ) -> Dict[str, Any]:
        started_at = datetime.now(timezone.utc).isoformat()
        products = self.feed_client.fetch_products(export_from, product_no, limit)

        results: List[ProductProcessResult] = []
        counts = {"processed": 0, "updated": 0, "deleted": 0, "skipped": 0, "failed": 0, "no_change": 0}

        for product in products:
            product_no_value = _get_product_no(product)
            counts["processed"] += 1
            if not product_no_value:
                results.append(
                    ProductProcessResult("", "skip", False, ["Missing productNo"], 0, 0)
                )
                counts["failed"] += 1
                continue

            self._log_unmapped(product, product_no_value)

            action = product.get("action")
            if action == "Delete":
                result = self._handle_delete(product_no_value, dry_run)
                results.append(result)
                if result.success:
                    counts["deleted"] += 1
                else:
                    counts["failed"] += 1
                continue

            result = self._handle_update(product, product_no_value, dry_run)
            results.append(result)
            if not result.success:
                counts["failed"] += 1
            elif result.action == "no_change":
                counts["no_change"] += 1
            else:
                counts["updated"] += 1

        finished_at = datetime.now(timezone.utc).isoformat()
        report = {
            "startedAt": started_at,
            "finishedAt": finished_at,
            "exportFrom": export_from,
            "dryRun": dry_run,
            "counts": counts,
            "products": [result.__dict__ for result in results],
        }
        report_path = Path(f"run_report_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json")
        report_path.write_text(json.dumps(report, ensure_ascii=True, indent=2), encoding="utf-8")

        if counts["failed"] == 0:
            self.state_store.write_now()

        return report

    def _handle_delete(self, product_no: str, dry_run: bool) -> ProductProcessResult:
        if dry_run:
            self.logger.info(
                "dry_run_delete",
                extra={"event": "dry_run_delete", "productNo": product_no},
            )
            return ProductProcessResult(product_no, "delete", True, [], 0, 0)

        try:
            self.jetshop_client.product_delete(product_no)
            self.logger.info(
                "product_deleted",
                extra={"event": "product_deleted", "productNo": product_no, "success": True},
            )
            return ProductProcessResult(product_no, "delete", True, [], 0, 0)
        except Exception as exc:
            self.logger.error(
                "product_delete_failed",
                extra={"event": "product_delete_failed", "productNo": product_no, "success": False, "detail": str(exc)},
            )
            return ProductProcessResult(product_no, "delete", False, [str(exc)], 0, 0)

    def _handle_update(self, product: Dict[str, Any], product_no: str, dry_run: bool) -> ProductProcessResult:
        errors: List[str] = []
        desired_by_culture, stock_data, categories, dynamic_fields, price_lists = self._build_desired(
            product, product_no, errors
        )
        if errors:
            self.logger.error(
                "mapping_failed",
                extra={"event": "mapping_failed", "productNo": product_no, "detail": "; ".join(errors)},
            )
            return ProductProcessResult(product_no, "skip", False, errors, 0, 0)

        try:
            current_by_culture = {}
            for culture in self.mapping.cultures:
                current_by_culture[culture] = self.jetshop_client.product_get(culture, product_no) or {}
        except Exception as exc:
            self.logger.error(
                "jetshop_read_failed",
                extra={"event": "jetshop_read_failed", "productNo": product_no, "success": False, "detail": str(exc)},
            )
            return ProductProcessResult(product_no, "read_failed", False, [str(exc)], 0, 0)

        current_dynamic: Dict[str, Dict[str, Any]] = {}

        diffs = []
        for culture in self.mapping.cultures:
            current = current_by_culture.get(culture) or {}
            desired = desired_by_culture.get(culture) or {}
            diffs.extend(diff_product_data(current, desired, culture))
            if categories is not None:
                diffs.extend(diff_categories(current.get("ProductInCategories", []), categories, culture))
            diffs.extend(diff_stock(current.get("StockData", {}), stock_data, culture))

        removed_categories: List[str] = []
        if categories is not None:
            base_current = current_by_culture.get(self.mapping.cultures[0], {}).get(
                "ProductInCategories", []
            )
            current_set = {str(item) for item in (base_current or [])}
            desired_set = {str(item) for item in categories}
            removed_categories = sorted(current_set - desired_set)

        dynamic_diffs = diff_dynamic_fields(current_dynamic, dynamic_fields)
        for price_item in price_lists:
            self.logger.info(
                "price_list_sync",
                extra={
                    "event": "price_list_sync",
                    "productNo": product_no,
                    "priceListId": price_item.get("PriceListId"),
                    "priceIncVat": price_item.get("PriceIncVat"),
                    "discountedPriceIncVat": price_item.get("DiscountedPriceIncVat"),
                },
            )

        for item in diffs + dynamic_diffs:
            self.logger.info(
                "field_change",
                extra={
                    "event": "field_change",
                    "productNo": product_no,
                    "culture": item.culture,
                    "targetField": item.target_field,
                    "oldValue": item.old_value,
                    "newValue": item.new_value,
                    "section": item.section,
                },
            )

        if not diffs and not dynamic_diffs and not price_lists:
            return ProductProcessResult(product_no, "no_change", True, [], 0, 0)

        if dry_run:
            diff_payload = {
                "productNo": product_no,
                "productDiffs": [item.__dict__ for item in diffs],
                "dynamicFieldDiffs": [item.__dict__ for item in dynamic_diffs],
                "priceLists": price_lists,
            }
            Path("diffs").mkdir(parents=True, exist_ok=True)
            Path(f"diffs/{product_no}.json").write_text(
                json.dumps(diff_payload, ensure_ascii=True, indent=2, default=_json_default),
                encoding="utf-8",
            )
            return ProductProcessResult(product_no, "dry_run", True, [], len(diffs), len(dynamic_diffs))

        try:
            if diffs:
                stock_payload = dict(stock_data)
                if stock_payload:
                    current_stock = (
                        current_by_culture.get(self.mapping.cultures[0], {}).get("StockData", {})
                    )
                    for key in ["UseAdvancedStatus", "StockStatusWhenOutOfStock"]:
                        if key not in stock_payload and current_stock.get(key) is not None:
                            stock_payload[key] = current_stock.get(key)

                template_id = getattr(self.jetshop_client, "template_id", None)
                if removed_categories:
                    reset_payloads = []
                    for culture in self.mapping.cultures:
                        reset_payload = {"ArticleNumber": product_no, "Culture": culture, "ProductInCategories": []}
                        if template_id:
                            reset_payload["TemplateId"] = template_id
                        reset_payloads.append(reset_payload)
                    self.logger.info(
                        "category_reset",
                        extra={
                            "event": "category_reset",
                            "productNo": product_no,
                            "removedCategories": removed_categories,
                        },
                    )
                    self.jetshop_client.product_add_update(reset_payloads)

                payloads = []
                for culture in self.mapping.cultures:
                    payload = dict(desired_by_culture.get(culture) or {})
                    if categories is not None:
                        payload["ProductInCategories"] = categories
                    if stock_payload:
                        payload["StockData"] = stock_payload
                    if template_id:
                        payload["TemplateId"] = template_id
                    payloads.append(payload)
                results = self.jetshop_client.product_add_update(payloads)
                failures = [res for res in results if not res.success]
                if failures:
                    error_msg = ", ".join([f"{res.culture}:{res.status}" for res in failures])
                    raise RuntimeError(f"Product_AddUpdate failed: {error_msg}")

            if dynamic_diffs:
                inputs = _build_dynamic_inputs(product_no, dynamic_fields, dynamic_diffs)
                dyn_results = self.jetshop_client.dyn_save(inputs)
                dyn_failures = [res for res in dyn_results if not res.success]
                if dyn_failures:
                    missing = [res for res in dyn_failures if _is_missing_dynamic_field(res.message)]
                    other_failures = [res for res in dyn_failures if res not in missing]
                    if other_failures:
                        error_msg = ", ".join([f"{res.key}:{res.message}" for res in other_failures])
                        raise RuntimeError(f"Dynamic field save failed: {error_msg}")
                    if missing:
                        self.logger.warning(
                            "dynamic_field_missing",
                            extra={
                                "event": "dynamic_field_missing",
                                "productNo": product_no,
                                "keys": [res.key for res in missing],
                            },
                        )

            if price_lists:
                self.jetshop_client.price_list_update(price_lists)

            return ProductProcessResult(product_no, "update", True, [], len(diffs), len(dynamic_diffs))
        except Exception as exc:
            self.logger.error(
                "product_update_failed",
                extra={"event": "product_update_failed", "productNo": product_no, "success": False, "detail": str(exc)},
            )
            return ProductProcessResult(product_no, "update", False, [str(exc)], len(diffs), len(dynamic_diffs))

    def _build_desired(
        self, product: Dict[str, Any], product_no: str, errors: List[str]
    ) -> Tuple[
        Dict[str, Dict[str, Any]],
        Dict[str, Any],
        Optional[List[str]],
        Dict[str, Dict[str, Any]],
        List[Dict[str, Any]],
    ]:
        attributes_by_code = {attr["importCode"]: attr for attr in product.get("attributes", [])}
        texts_by_code = {text["importCode"]: text for text in product.get("texts", [])}

        desired_by_culture: Dict[str, Dict[str, Any]] = {}
        for culture in self.mapping.cultures:
            data = {"ArticleNumber": product_no, "Culture": culture}
            for entry in self.mapping.product_fields:
                if entry.cultures and culture not in entry.cultures:
                    continue
                value = _apply_mapping_entry(
                    entry,
                    product,
                    attributes_by_code,
                    texts_by_code,
                    self.mapping,
                    culture,
                    self.logger,
                    errors,
                )
                if value is None:
                    continue
                data[entry.target] = value
            desired_by_culture[culture] = data

        stock_data: Dict[str, Any] = {}
        for entry in self.mapping.stock_fields:
            value = _apply_mapping_entry(
                entry,
                product,
                attributes_by_code,
                texts_by_code,
                self.mapping,
                None,
                self.logger,
                errors,
            )
            if value is None:
                continue
            stock_data[entry.target] = value

        categories = _extract_categories(
            self.mapping.category_fields,
            product,
            attributes_by_code,
            self.mapping,
            self.logger,
            errors,
        )

        dynamic_fields: Dict[str, Dict[str, Any]] = {}
        for entry in self.mapping.dynamic_fields_allowlist:
            cultures = entry.cultures or self.mapping.cultures
            for culture in cultures:
                value = _apply_dynamic_mapping(
                    entry,
                    product,
                    attributes_by_code,
                    texts_by_code,
                    self.mapping,
                    culture,
                    self.logger,
                    errors,
                )
                if value is None:
                    continue
                dynamic_fields.setdefault(entry.key, {})[culture] = value

        price_lists = _build_price_list_items(
            self.mapping,
            product_no,
            product,
            attributes_by_code,
            errors,
            self.logger,
        )

        return desired_by_culture, stock_data, categories, dynamic_fields, price_lists

    def _log_unmapped(self, product: Dict[str, Any], product_no: str) -> None:
        mapped_attrs = set(self.mapping.mapped_attribute_codes())
        mapped_texts = set(self.mapping.mapped_text_codes())
        feed_attrs = {attr.get("importCode") for attr in product.get("attributes", []) if attr.get("importCode")}
        feed_texts = {text.get("importCode") for text in product.get("texts", []) if text.get("importCode")}
        unmapped_attrs = sorted(feed_attrs - mapped_attrs)
        unmapped_texts = sorted(feed_texts - mapped_texts)
        if unmapped_attrs or unmapped_texts:
            self.logger.info(
                "unmapped_fields",
                extra={
                    "event": "unmapped_fields",
                    "productNo": product_no,
                    "unmappedAttributes": unmapped_attrs,
                    "unmappedTexts": unmapped_texts,
                },
            )


def _get_product_no(product: Dict[str, Any]) -> Optional[str]:
    identifier = product.get("identifier") or {}
    return identifier.get("productNo")


def _apply_mapping_entry(
    entry: FieldMapping,
    product: Dict[str, Any],
    attributes_by_code: Dict[str, Dict[str, Any]],
    texts_by_code: Dict[str, Dict[str, Any]],
    mapping: MappingConfig,
    culture: Optional[str],
    logger,
    errors: List[str],
) -> Any:
    source = _select_source(entry, culture)
    raw_value, attribute = _resolve_source(source, product, attributes_by_code, texts_by_code)
    value = raw_value

    if attribute and isinstance(raw_value, dict) and "value" in raw_value:
        value = raw_value.get("value")

    if culture and isinstance(value, dict):
        value = _select_localized(value, mapping, culture, entry.fallback)

    if not entry.allow_empty and is_empty(value):
        if entry.optional or entry.preserve_if_missing:
            return None
        errors.append(f"{entry.target}: missing required value")
        return None

    value = _coerce_with_policy(value, entry.type, entry.coerce, entry.item_type, entry.target, logger, errors)

    fallback_culture = entry.fallback or (mapping.fallbacks.get(culture) if culture else None)
    context = TransformContext(
        culture=culture,
        feed_language=mapping.culture_map.get(culture) if culture else None,
        fallback_language=mapping.culture_map.get(fallback_culture) if fallback_culture else None,
        attribute=attribute,
    )
    value = apply_transforms(value, entry.transforms, context)

    try:
        validate_constraints(value, entry.validations, entry.target)
    except ValidationError as exc:
        errors.append(str(exc))
        return None

    if not entry.allow_empty and is_empty(value):
        if entry.optional or entry.preserve_if_missing:
            return None
        errors.append(f"{entry.target}: empty value not allowed")
        return None

    return value


def _apply_dynamic_mapping(
    entry: DynamicFieldMapping,
    product: Dict[str, Any],
    attributes_by_code: Dict[str, Dict[str, Any]],
    texts_by_code: Dict[str, Dict[str, Any]],
    mapping: MappingConfig,
    culture: str,
    logger,
    errors: List[str],
) -> Any:
    source = _select_source(entry, culture)
    raw_value, attribute = _resolve_source(source, product, attributes_by_code, texts_by_code)
    value = raw_value

    if attribute and isinstance(raw_value, dict) and "value" in raw_value:
        value = raw_value.get("value")

    if isinstance(value, dict):
        value = _select_localized(value, mapping, culture, entry.fallback)

    if not entry.allow_empty and is_empty(value):
        if entry.optional:
            return None
        errors.append(f"{entry.key}: missing required value")
        return None

    value = _coerce_with_policy(value, entry.type, entry.coerce, entry.item_type, entry.key, logger, errors)

    fallback_culture = entry.fallback or mapping.fallbacks.get(culture)
    context = TransformContext(
        culture=culture,
        feed_language=mapping.culture_map.get(culture),
        fallback_language=mapping.culture_map.get(fallback_culture) if fallback_culture else None,
        attribute=attribute,
    )
    value = apply_transforms(value, entry.transforms, context)

    try:
        validate_constraints(value, entry.validations, entry.key)
    except ValidationError as exc:
        errors.append(str(exc))
        return None

    if not entry.allow_empty and is_empty(value):
        if entry.optional:
            return None
        errors.append(f"{entry.key}: empty value not allowed")
        return None

    return value


def _extract_categories(
    category_mapping,
    product: Dict[str, Any],
    attributes_by_code: Dict[str, Dict[str, Any]],
    mapping: MappingConfig,
    logger,
    errors: List[str],
) -> Optional[List[str]]:
    raw_value, _attribute = _resolve_source(category_mapping.source, product, attributes_by_code, {})
    if raw_value is None:
        if category_mapping.optional:
            return None
        errors.append("Category mapping missing required value")
        return []

    categories = _coerce_with_policy(
        raw_value,
        category_mapping.type,
        category_mapping.coerce,
        category_mapping.item_type,
        "categories",
        logger,
        errors,
    )
    if isinstance(categories, list):
        return [str(item) for item in categories]
    return [str(categories)]


def _build_price_list_items(
    mapping: MappingConfig,
    product_no: str,
    product: Dict[str, Any],
    attributes_by_code: Dict[str, Dict[str, Any]],
    errors: List[str],
    logger,
) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for entry in mapping.price_lists:
        raw_value, _attribute = _resolve_source(entry.price_source, product, attributes_by_code, {})
        value = raw_value.get("value") if isinstance(raw_value, dict) else raw_value
        if is_empty(value):
            if entry.optional:
                continue
            errors.append(f"price_list:{entry.price_list_id} missing price")
            continue

        price_value = _coerce_with_policy(
            value,
            entry.type,
            entry.coerce,
            None,
            entry.name or entry.price_list_id,
            logger,
            errors,
        )
        if price_value is None:
            continue

        item: Dict[str, Any] = {
            "ArticleNumber": product_no,
            "PriceListId": entry.price_list_id,
            "PriceIncVat": price_value,
        }

        if entry.discounted_price_source:
            disc_raw, _attribute = _resolve_source(
                entry.discounted_price_source, product, attributes_by_code, {}
            )
            disc_value = disc_raw.get("value") if isinstance(disc_raw, dict) else disc_raw
            empty_discount = is_empty(disc_value)
            if entry.clear_discount_on_missing and not empty_discount:
                if isinstance(disc_value, (int, float)) and disc_value == 0:
                    empty_discount = True
                elif isinstance(disc_value, str) and disc_value.strip() in {"0", "0.0", "0.00"}:
                    empty_discount = True

            if empty_discount:
                if entry.clear_discount_on_missing:
                    item["DiscountedPriceIncVat"] = -1
            else:
                discount_value = _coerce_with_policy(
                    disc_value,
                    entry.type,
                    entry.coerce,
                    None,
                    f"{entry.name or entry.price_list_id}_discount",
                    logger,
                    errors,
                )
                if discount_value is not None:
                    item["DiscountedPriceIncVat"] = discount_value
        elif entry.clear_discount_on_missing:
            item["DiscountedPriceIncVat"] = -1

        if entry.hide_product_source:
            show_raw, _attribute = _resolve_source(
                entry.hide_product_source, product, attributes_by_code, {}
            )
            show_value = show_raw.get("value") if isinstance(show_raw, dict) else show_raw
            if not is_empty(show_value):
                show_bool = _coerce_with_policy(
                    show_value,
                    "bool",
                    entry.coerce,
                    None,
                    f"{entry.name or entry.price_list_id}_hide",
                    logger,
                    errors,
                )
                if show_bool is not None:
                    item["HideProduct"] = not bool(show_bool)

        items.append(item)

    return items


def _select_source(entry: Any, culture: Optional[str]) -> str:
    if entry.source_by_culture and culture:
        if culture in entry.source_by_culture:
            return entry.source_by_culture[culture]
        fallback = None
        if entry.fallback_by_culture:
            fallback = entry.fallback_by_culture.get(culture)
        if fallback and fallback in entry.source_by_culture:
            return entry.source_by_culture[fallback]
    if entry.source:
        return entry.source
    raise ValueError("No source available for mapping entry")


def _resolve_source(
    source: str,
    product: Dict[str, Any],
    attributes_by_code: Dict[str, Dict[str, Any]],
    texts_by_code: Dict[str, Dict[str, Any]],
) -> Tuple[Any, Optional[Dict[str, Any]]]:
    root, key, path = parse_source_selector(source)
    if root == "attributes":
        attribute = attributes_by_code.get(key or "")
        if not attribute:
            return None, None
        value: Any = attribute
        for segment in path:
            if isinstance(value, dict):
                value = value.get(segment)
            else:
                value = None
                break
        return value, attribute

    if root == "texts":
        text = texts_by_code.get(key or "")
        if not text:
            return None, None
        value = text
        for segment in path:
            if isinstance(value, dict):
                value = value.get(segment)
            else:
                value = None
                break
        return value, None

    value = product.get(root)
    for segment in path:
        if isinstance(value, dict):
            value = value.get(segment)
        else:
            value = None
            break
    return value, None


def _select_localized(value: Dict[str, Any], mapping: MappingConfig, culture: str, fallback: Optional[str]) -> Any:
    feed_lang = mapping.culture_map.get(culture)
    fallback_culture = fallback or mapping.fallbacks.get(culture)
    fallback_lang = mapping.culture_map.get(fallback_culture) if fallback_culture else None

    def pick(key: Optional[str]) -> Any:
        if not key or key not in value:
            return None
        selected = value.get(key)
        if is_empty(selected):
            return None
        return selected

    for key in (feed_lang, culture):
        selected = pick(key)
        if selected is not None:
            return selected

    for key in (fallback_lang, fallback_culture):
        selected = pick(key)
        if selected is not None:
            return selected

    return None


def _coerce_with_policy(
    value: Any,
    expected_type: str,
    policy: str,
    item_type: Optional[str],
    field_name: str,
    logger,
    errors: List[str],
) -> Any:
    if value is None:
        return None
    try:
        return coerce_value(value, expected_type, policy, item_type)
    except ValidationError as exc:
        if policy == "coerce":
            logger.warning(
                "coerce_failed",
                extra={"event": "coerce_failed", "field": field_name, "detail": exc.message},
            )
            return value
        errors.append(f"{field_name}: {exc.message}")
        return None


def _build_dynamic_inputs(
    product_no: str,
    dynamic_fields: Dict[str, Dict[str, Any]],
    dynamic_diffs: List[Any],
) -> List[Dict[str, Any]]:
    changed_keys = {item.target_field for item in dynamic_diffs}
    inputs: List[Dict[str, Any]] = []
    for key, values in dynamic_fields.items():
        if key not in changed_keys:
            continue
        item_values = []
        for culture, value in values.items():
            item_values.append({"Culture": culture, "Value": value})
        inputs.append({"ArticleNumber": product_no, "Key": key, "ItemValues": item_values})
    return inputs


def _json_default(value: Any) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def _is_missing_dynamic_field(message: str) -> bool:
    if not message:
        return False
    normalized = message.lower()
    return "no dynamic field" in normalized and "connected to product" in normalized
