import logging

import pytest

from src.config import Config
from src.jetshop_client import (
    SoapFaultError,
    JetshopClient,
    NIL_VALUE,
    _build_dynamic_input_xml,
    _build_envelope,
    _build_product_data_xml,
    _build_price_list_item_xml,
    _raise_on_fault,
)


def test_build_product_data_xml():
    xml = _build_product_data_xml(
        {
            "ArticleNumber": "Pelle-1092-10",
            "Culture": "sv-SE",
            "TemplateId": "1",
            "Name": "Nigella",
            "ShortDescription": "Short",
            "ProductDescription": "Long",
            "Price": "10.0000",
            "EanCode": "123",
            "ProductInCategories": ["150", "151"],
            "StockData": {"NewStockCount": 3, "UseAdvancedStatus": True},
        }
    )
    assert "<ArticleNumber>Pelle-1092-10</ArticleNumber>" in xml
    assert "<TemplateId>1</TemplateId>" in xml
    assert "<ProductInCategories>" in xml
    assert "<CategoryId>150</CategoryId>" in xml
    assert "<StockData>" in xml
    assert "<NewStockCount>3</NewStockCount>" in xml
    assert "<UseAdvancedStatus>true</UseAdvancedStatus>" in xml


def test_build_product_data_xml_empty_categories():
    xml = _build_product_data_xml(
        {
            "ArticleNumber": "Pelle-1092-10",
            "Culture": "sv-SE",
            "ProductInCategories": [],
        }
    )
    assert "<ProductInCategories" in xml
    assert "<ProductInCategories />" in xml


def test_build_dynamic_input_xml():
    xml = _build_dynamic_input_xml(
        {
            "ArticleNumber": "Pelle-1092-10",
            "Key": "atr_colour",
            "ItemValues": [{"Culture": "sv-SE", "Value": "Vit"}],
        }
    )
    assert "<Key>atr_colour</Key>" in xml
    assert "<Culture>sv-SE</Culture>" in xml
    assert "<Value>Vit</Value>" in xml


def test_build_price_list_item_xml():
    xml = _build_price_list_item_xml(
        {
            "ArticleNumber": "Pelle-1092-10",
            "PriceListId": "guid-1",
            "PriceIncVat": 135,
            "DiscountedPriceIncVat": 99,
        }
    )
    assert "<ArticleNumber>Pelle-1092-10</ArticleNumber>" in xml
    assert "<PriceListId>guid-1</PriceListId>" in xml
    assert "<PriceIncVat>135</PriceIncVat>" in xml
    assert "<DiscountedPriceIncVat>99</DiscountedPriceIncVat>" in xml


def test_build_price_list_item_xml_nil():
    xml = _build_price_list_item_xml(
        {
            "ArticleNumber": "Pelle-1092-10",
            "PriceListId": "guid-1",
            "PriceIncVat": 135,
            "DiscountedPriceIncVat": -1,
        }
    )
    assert "<DiscountedPriceIncVat>-1</DiscountedPriceIncVat>" in xml


def test_build_envelope_includes_header():
    body = "<Test>value</Test>"
    header = "<soap12:Header><ShopId>1</ShopId></soap12:Header>"
    xml = _build_envelope(body, header)
    assert header in xml
    assert body in xml


def test_raise_on_fault():
    xml = (
        '<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope">'
        "<soap:Body>"
        "<soap:Fault>"
        "<soap:Code><soap:Value>soap:Sender</soap:Value></soap:Code>"
        "<soap:Reason><soap:Text>Bad</soap:Text></soap:Reason>"
        "</soap:Fault>"
        "</soap:Body>"
        "</soap:Envelope>"
    )
    with pytest.raises(SoapFaultError):
        _raise_on_fault(xml)


def test_product_get_builds_product_options(monkeypatch):
    config = Config(
        feed_token_url="https://example.invalid/token",
        feed_client_id="client",
        feed_client_secret="secret",
        feed_export_url="https://example.invalid/export",
        jetshop_soap_url="https://example.invalid/soap",
        jetshop_username="user",
        jetshop_password="pass",
        jetshop_shop_id="1",
        jetshop_soap_header_xml=None,
        jetshop_template_id="1",
        cultures=["sv-SE", "nb-NO"],
        log_file="logs/test.log",
        mapping_file="mappings/mapping.yaml",
        log_level="INFO",
        http_timeout=5,
        retry_count=1,
        retry_backoff=0.1,
    )
    logger = logging.getLogger("test_product_get")
    client = JetshopClient(config, logger)

    captured = {}

    def fake_post(body_xml, operation):
        captured["body"] = body_xml
        return (
            '<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope">'
            '<soap:Body>'
            '<Product_GetResponse xmlns="WebServiceProvider">'
            "<Product_GetResult>"
            "<ProductData>"
            "<ArticleNumber>Pelle-1092-10</ArticleNumber>"
            "<Culture>sv-SE</Culture>"
            "</ProductData>"
            "</Product_GetResult>"
            "</Product_GetResponse>"
            "</soap:Body>"
            "</soap:Envelope>"
        )

    monkeypatch.setattr(client, "_post_soap", fake_post)

    client.product_get("sv-SE", "Pelle-1092-10")

    assert "<productOptions>" in captured["body"]
    assert "<ArticleNumbers>" in captured["body"]
    assert "<string>Pelle-1092-10</string>" in captured["body"]
    assert "<Culture>sv-SE</Culture>" in captured["body"]


def test_price_list_update_builds_body(monkeypatch):
    config = Config(
        feed_token_url="https://example.invalid/token",
        feed_client_id="client",
        feed_client_secret="secret",
        feed_export_url="https://example.invalid/export",
        jetshop_soap_url="https://example.invalid/soap",
        jetshop_username="user",
        jetshop_password="pass",
        jetshop_shop_id="1",
        jetshop_soap_header_xml=None,
        jetshop_template_id="1",
        cultures=["sv-SE", "nb-NO"],
        log_file="logs/test.log",
        mapping_file="mappings/mapping.yaml",
        log_level="INFO",
        http_timeout=5,
        retry_count=1,
        retry_backoff=0.1,
    )
    logger = logging.getLogger("test_price_list_update")
    client = JetshopClient(config, logger)

    captured = {}

    def fake_post(body_xml, operation):
        captured["body"] = body_xml
        captured["operation"] = operation
        return (
            '<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope">'
            "<soap:Body></soap:Body>"
            "</soap:Envelope>"
        )

    monkeypatch.setattr(client, "_post_soap", fake_post)

    client.price_list_update(
        [
            {
                "ArticleNumber": "Pelle-1092-10",
                "PriceListId": "guid-1",
                "PriceIncVat": 135,
                "DiscountedPriceIncVat": 99,
            }
        ]
    )

    assert captured["operation"] == "PriceList_UpdateArticleIncVAT"
    assert "<PriceList_UpdateArticleIncVAT" in captured["body"]
    assert "<PriceListId>guid-1</PriceListId>" in captured["body"]
