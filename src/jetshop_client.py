"""Jetshop SOAP API client."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import time
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

import requests
from xml.sax.saxutils import escape as escape_xml

from .config import Config
from .http_utils import request_with_retry


SOAP_ENV_NS = "http://www.w3.org/2003/05/soap-envelope"
WS_NS = "WebServiceProvider"
NS = {"soap": SOAP_ENV_NS, "ws": WS_NS}


class NilValueType:
    pass


NIL_VALUE = NilValueType()


class SoapFaultError(Exception):
    def __init__(self, code: str, reason: str) -> None:
        super().__init__(f"SOAP Fault: {code} - {reason}")
        self.code = code
        self.reason = reason


@dataclass
class ProductResult:
    article_number: str
    culture: str
    status: str
    success: bool


@dataclass
class DynamicFieldResult:
    key: str
    success: bool
    message: str


class JetshopClient:
    def __init__(self, config: Config, logger) -> None:
        self.config = config
        self.logger = logger
        self.session = requests.Session()
        self.session.auth = (config.jetshop_username, config.jetshop_password)
        self.header_xml = _build_header_xml(config)
        self.template_id = config.jetshop_template_id

    def product_get(self, culture: str, article_number: str) -> Optional[Dict[str, Any]]:
        body = f"""
<Product_Get xmlns="{WS_NS}">
  <productOptions>
    <ArticleNumber>{escape_xml(article_number)}</ArticleNumber>
    <Culture>{escape_xml(culture)}</Culture>
  </productOptions>
</Product_Get>
""".strip()
        response_xml = self._post_soap(body, "Product_Get")
        root = ET.fromstring(response_xml)
        product_data = _find_product_data(root, article_number)
        if product_data is None:
            return None

        result = {
            "ArticleNumber": _text_any_ns(product_data, "ArticleNumber"),
            "Culture": _text_any_ns(product_data, "Culture"),
            "Name": _text_any_ns(product_data, "Name"),
            "SubName": _text_any_ns(product_data, "SubName"),
            "ShortDescription": _text_any_ns(product_data, "ShortDescription"),
            "ProductDescription": _text_any_ns(product_data, "ProductDescription"),
            "Price": _text_any_ns(product_data, "Price"),
            "EanCode": _text_any_ns(product_data, "EanCode"),
            "ProductInCategories": _parse_categories(product_data),
            "StockData": _parse_stock(product_data),
        }
        self.logger.debug(
            "jetshop_categories_current",
            extra={
                "event": "jetshop_categories_current",
                "productNo": article_number,
                "culture": culture,
                "categories": result.get("ProductInCategories", []),
            },
        )
        return result

    def product_add_update(self, product_data_list: List[Dict[str, Any]]) -> List[ProductResult]:
        for item in product_data_list:
            if "ProductInCategories" in item:
                categories_xml = _build_categories_xml(item)
                self.logger.debug(
                    "category_payload_xml",
                    extra={
                        "event": "category_payload_xml",
                        "productNo": item.get("ArticleNumber"),
                        "culture": item.get("Culture"),
                        "categoryXml": categories_xml,
                    },
                )
        products_xml = "\n".join([_build_product_data_xml(item) for item in product_data_list])
        body = f"""
<Product_AddUpdate xmlns="{WS_NS}">
  <products>
    {products_xml}
  </products>
</Product_AddUpdate>
""".strip()
        response_xml = self._post_soap(body, "Product_AddUpdate")
        root = ET.fromstring(response_xml)
        results: List[ProductResult] = []
        for item in root.findall(".//ws:ProductResult", NS):
            article_number = _text(item, "ArticleNumber") or ""
            culture = _text(item, "Culture") or ""
            status = _text(item, "StatusMainProductCreateDelete") or ""
            success = status in {"SuccessNew", "SuccessUpdate"}
            results.append(ProductResult(article_number, culture, status, success))
        return results

    def product_delete(self, article_number: str) -> None:
        body = f"""
<Product_Delete xmlns="{WS_NS}">
  <productDeleteRequest>
    <ArticleNumber>{escape_xml(article_number)}</ArticleNumber>
    <AddRedirect>false</AddRedirect>
    <Redirects />
  </productDeleteRequest>
</Product_Delete>
""".strip()
        response_xml = self._post_soap(body, "Product_Delete")
        if "NotFound" in response_xml:
            self.logger.info(
                "jetshop_delete_not_found",
                extra={"event": "jetshop_delete_not_found", "productNo": article_number},
            )

    def dyn_get(self, article_numbers: List[str], cultures: List[str]) -> Dict[str, Dict[str, Any]]:
        articles_xml = "\n".join([f"<string>{escape_xml(num)}</string>" for num in article_numbers])
        cultures_xml = "\n".join([f"<string>{escape_xml(culture)}</string>" for culture in cultures])
        body = f"""
<ProductDynamicField_GetProductDynamicFieldData xmlns="{WS_NS}">
  <articleNumbers>
    {articles_xml}
  </articleNumbers>
  <cultures>
    {cultures_xml}
  </cultures>
</ProductDynamicField_GetProductDynamicFieldData>
""".strip()
        response_xml = self._post_soap(body, "ProductDynamicField_GetProductDynamicFieldData")
        root = ET.fromstring(response_xml)
        result: Dict[str, Dict[str, Any]] = {}
        for item in root.findall(".//ws:DynamicFieldOnProductOutput", NS):
            key = _text(item, "Key")
            if not key:
                continue
            values = result.setdefault(key, {})
            for loc in item.findall(".//ws:Localization", NS):
                culture = _text(loc, "Culture")
                value = _text(loc, "Value")
                if culture:
                    values[culture] = value
        return result

    def dyn_save(self, inputs: List[Dict[str, Any]]) -> List[DynamicFieldResult]:
        input_xml = "\n".join([_build_dynamic_input_xml(item) for item in inputs])
        body = f"""
<ProductDynamicField_SaveProductDynamicFieldData xmlns="{WS_NS}">
  <dynamicFieldOnProductInputs>
    {input_xml}
  </dynamicFieldOnProductInputs>
</ProductDynamicField_SaveProductDynamicFieldData>
""".strip()
        response_xml = self._post_soap(body, "ProductDynamicField_SaveProductDynamicFieldData")
        root = ET.fromstring(response_xml)
        results: List[DynamicFieldResult] = []
        for item in root.findall(".//ws:DynamicFieldItemResult", NS):
            key = _text(item, "Key") or ""
            success_text = _text(item, "Success") or "false"
            success = success_text.lower() == "true"
            message = _text(item, "Message") or ""
            results.append(DynamicFieldResult(key, success, message))
        return results

    def price_list_update(self, inputs: List[Dict[str, Any]]) -> None:
        input_xml = "\n".join([_build_price_list_item_xml(item) for item in inputs])
        body = f"""
<PriceList_UpdateArticleIncVAT xmlns="{WS_NS}">
  <articlePriceLists>
    {input_xml}
  </articlePriceLists>
</PriceList_UpdateArticleIncVAT>
""".strip()
        self._post_soap(body, "PriceList_UpdateArticleIncVAT")

    def _post_soap(self, body_xml: str, operation: str) -> str:
        envelope = _build_envelope(body_xml, self.header_xml)
        start = time.monotonic()
        success = False
        error_message = None
        status_code = None
        response_snippet = None
        try:
            response = request_with_retry(
                self.session,
                "POST",
                self.config.jetshop_soap_url,
                logger=self.logger,
                timeout=self.config.http_timeout,
                retries=self.config.retry_count,
                backoff=self.config.retry_backoff,
                data=envelope.encode("utf-8"),
                headers={
                    "Content-Type": "application/soap+xml; charset=utf-8",
                    "Accept": "application/soap+xml",
                },
            )
            status_code = response.status_code
            response_text = response.text
            _raise_on_fault(response_text)
            if response.status_code >= 400:
                response_snippet = response_text[:800]
                response.raise_for_status()
            success = True
            return response_text
        except Exception as exc:
            error_message = str(exc)
            raise
        finally:
            duration_ms = int((time.monotonic() - start) * 1000)
            log_fn = self.logger.info if success else self.logger.error
            extra = {
                "event": "jetshop_request",
                "operation": operation,
                "durationMs": duration_ms,
                "success": success,
                "statusCode": status_code,
            }
            if error_message:
                extra["detail"] = error_message
            if response_snippet:
                extra["responseSnippet"] = response_snippet
            log_fn("jetshop_request", extra=extra)


def _build_header_xml(config: Config) -> str:
    header = config.jetshop_soap_header_xml
    if header:
        if "<soap12:Header" in header or "<soap:Header" in header:
            return header
        return f"<soap12:Header>{header}</soap12:Header>"
    return f"<soap12:Header><ShopId>{escape_xml(config.jetshop_shop_id)}</ShopId></soap12:Header>"


def _build_envelope(body_xml: str, header_xml: str) -> str:
    return f"""<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 xmlns:soap12="{SOAP_ENV_NS}">
  {header_xml}
  <soap12:Body>
    {body_xml}
  </soap12:Body>
</soap12:Envelope>"""


def _raise_on_fault(response_xml: str) -> None:
    root = ET.fromstring(response_xml)
    fault = None
    for elem in root.iter():
        if elem.tag.endswith("Fault"):
            fault = elem
            break
    if fault is None:
        return
    code = fault.findtext(".//faultcode") or fault.findtext(".//soap:Value", namespaces=NS) or "Fault"
    reason = fault.findtext(".//faultstring") or fault.findtext(".//soap:Text", namespaces=NS) or "Unknown"
    raise SoapFaultError(code, reason)


def _text(parent: ET.Element, tag: str) -> Optional[str]:
    element = parent.find(f"ws:{tag}", NS)
    if element is None or element.text is None:
        return None
    return element.text


def _parse_categories(product_data: ET.Element) -> List[str]:
    categories: List[str] = []
    for item in product_data.iter():
        if not item.tag.endswith("ProductInCategoryData"):
            continue
        category_id = _find_text_any_ns(item, "CategoryId")
        if not category_id:
            continue
        categories.append(category_id.strip() if isinstance(category_id, str) else category_id)
    return categories


def _parse_stock(product_data: ET.Element) -> Dict[str, Any]:
    stock_node = product_data.find(".//ws:StockData", NS)
    if stock_node is None:
        return {}
    return {
        "DeliveryDate": _text(stock_node, "DeliveryDate"),
        "NewStockCount": _parse_int(_text(stock_node, "NewStockCount")),
        "StockStatusId": _parse_int(_text(stock_node, "StockStatusId")),
        "StockStatusName": _text(stock_node, "StockStatusName"),
        "UseAdvancedStatus": _parse_bool(_text(stock_node, "UseAdvancedStatus")),
        "StockStatusWhenOutOfStock": _parse_int(_text(stock_node, "StockStatusWhenOutOfStock")),
    }


def _find_text_any_ns(parent: ET.Element, tag_suffix: str) -> Optional[str]:
    for child in parent.iter():
        if child.tag.endswith(tag_suffix):
            return child.text
    return None


def _text_any_ns(parent: ET.Element, tag: str) -> Optional[str]:
    value = _text(parent, tag)
    if value is not None:
        return value
    return _child_text_any_ns(parent, tag)


def _child_text_any_ns(parent: ET.Element, tag_suffix: str) -> Optional[str]:
    for child in list(parent):
        if child.tag.endswith(tag_suffix):
            return child.text
    return None


def _find_product_data(root: ET.Element, article_number: str) -> Optional[ET.Element]:
    candidates = root.findall(".//ws:ProductData", NS)
    if not candidates:
        candidates = [node for node in root.iter() if node.tag.endswith("ProductData")]
    if not candidates:
        return None
    for node in candidates:
        article = _child_text_any_ns(node, "ArticleNumber")
        if article == article_number:
            return node
    return candidates[0]


def _parse_int(value: Optional[str]) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def _parse_bool(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None
    lowered = value.strip().lower()
    if lowered in {"true", "1", "yes"}:
        return True
    if lowered in {"false", "0", "no"}:
        return False
    return None


def _build_product_data_xml(product_data: Dict[str, Any]) -> str:
    fields = []
    for key in [
        "ArticleNumber",
        "Culture",
        "TemplateId",
        "Name",
        "SubName",
        "ShortDescription",
        "ProductDescription",
        "Price",
        "EanCode",
    ]:
        value = product_data.get(key)
        if value is None:
            continue
        fields.append(f"<{key}>{escape_xml(_format_xml_value(value))}</{key}>")

    categories_xml = _build_categories_xml(product_data)

    stock_data = product_data.get("StockData") or {}
    stock_xml = ""
    if stock_data:
        stock_fields = []
        for key, value in stock_data.items():
            if value is None:
                continue
            if value is NIL_VALUE:
                stock_fields.append(f"<{key} xsi:nil=\"true\" />")
            else:
                stock_fields.append(f"<{key}>{escape_xml(_format_xml_value(value))}</{key}>")
        if stock_fields:
            article_number = escape_xml(str(product_data.get("ArticleNumber", "")))
            stock_fields.insert(0, f"<ArticleNumber>{article_number}</ArticleNumber>")
            stock_xml = f"<StockData>{''.join(stock_fields)}</StockData>"

    return f"<ProductData>{''.join(fields)}{categories_xml}{stock_xml}</ProductData>"


def _build_categories_xml(product_data: Dict[str, Any]) -> str:
    categories = product_data.get("ProductInCategories")
    if categories is None:
        return ""
    if not categories:
        return "<ProductInCategories />"

    category_items = []
    for entry in categories:
        if isinstance(entry, dict):
            category_id = entry.get("CategoryId")
            product_id = entry.get("ProductId")
            state = entry.get("ProductInCategoryState")
            if state:
                sort_order = entry.get("SortOrder")
                is_canonical = entry.get("IsCanonical")
            else:
                sort_order = entry.get("SortOrder", 0)
                is_canonical = entry.get("IsCanonical", False)
        else:
            category_id = entry
            product_id = None
            sort_order = 0
            is_canonical = False
            state = None

        if not category_id:
            continue

        category_fields = [
            f"<ArticleNumber>{escape_xml(str(product_data.get('ArticleNumber', '')))}</ArticleNumber>",
            f"<CategoryId>{escape_xml(str(category_id))}</CategoryId>",
        ]
        if product_id is not None:
            category_fields.append(f"<ProductId>{escape_xml(_format_xml_value(product_id))}</ProductId>")
        if sort_order is not None:
            category_fields.append(f"<SortOrder>{escape_xml(_format_xml_value(sort_order))}</SortOrder>")
        if is_canonical is not None:
            category_fields.append(f"<IsCanonical>{escape_xml(_format_xml_value(is_canonical))}</IsCanonical>")
        if state is not None:
            category_fields.append(
                f"<ProductInCategoryState>{escape_xml(_format_xml_value(state))}</ProductInCategoryState>"
            )
        category_items.append(f"<ProductInCategoryData>{''.join(category_fields)}</ProductInCategoryData>")

    return f"<ProductInCategories>{''.join(category_items)}</ProductInCategories>"


def _build_dynamic_input_xml(item: Dict[str, Any]) -> str:
    article_number = escape_xml(str(item.get("ArticleNumber", "")))
    key = escape_xml(str(item.get("Key", "")))
    localizations = []
    for loc in item.get("ItemValues", []):
        culture = escape_xml(str(loc.get("Culture", "")))
        value = loc.get("Value")
        if value is None:
            continue
        localizations.append(
            f"<Localization><Culture>{culture}</Culture><Value>{escape_xml(_format_xml_value(value))}</Value></Localization>"
        )
    item_values_xml = f"<ItemValues>{''.join(localizations)}</ItemValues>" if localizations else "<ItemValues />"
    return f"""
<DynamicFieldOnProductInput>
  <ArticleNumber>{article_number}</ArticleNumber>
  <Key>{key}</Key>
  <ClearExistingListData>false</ClearExistingListData>
  {item_values_xml}
  <DynamicFieldItemListData />
  <DynamicFieldItemMultiLevelListData />
</DynamicFieldOnProductInput>
""".strip()


def _format_xml_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def _build_price_list_item_xml(item: Dict[str, Any]) -> str:
    fields = []
    for key in [
        "ArticleNumber",
        "PriceListId",
        "PriceIncVat",
        "DiscountedPriceIncVat",
        "HideProduct",
        "DiscountedPriceIsMemberPrice",
        "UseDiscountDateSpan",
        "DiscountStartDate",
        "DiscountEndDate",
    ]:
        value = item.get(key)
        if value is None:
            continue
        if value is NIL_VALUE:
            fields.append(f"<{key} xsi:nil=\"true\" />")
        else:
            fields.append(f"<{key}>{escape_xml(_format_xml_value(value))}</{key}>")
    return f"<ArticlePriceListIncVat>{''.join(fields)}</ArticlePriceListIncVat>"
