from src.mapping_loader import load_mapping, parse_source_selector


def test_load_mapping():
    mapping = load_mapping("mappings/mapping.yaml")
    assert mapping.version == 1
    assert "sv-SE" in mapping.cultures
    assert any(entry.target == "ArticleNumber" for entry in mapping.product_fields)
    assert any(entry.key == "atr_colour" for entry in mapping.dynamic_fields_allowlist)
    assert mapping.dynamic_fields_auto_map.enabled is True
    assert "atr_dia" in mapping.dynamic_fields_auto_map.allowed_keys
    assert "b2c_mp" in mapping.dynamic_fields_auto_map.allowed_keys
    assert len(mapping.price_lists) == 3


def test_parse_source_selector():
    root, key, path = parse_source_selector("attributes[atr_colour].value.sv")
    assert root == "attributes"
    assert key == "atr_colour"
    assert path == ["value", "sv"]

    root, key, path = parse_source_selector("identifier.productNo")
    assert root == "identifier"
    assert key is None
    assert path == ["productNo"]
