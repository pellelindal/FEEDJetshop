from src.transformers import TransformContext, data_register_label, format_price, newline_to_br


def test_newline_to_br():
    context = TransformContext(culture="sv-SE", feed_language="sv", fallback_language="sv", attribute=None)
    assert newline_to_br("line1\nline2", context) == "line1<br>line2"


def test_format_price():
    context = TransformContext(culture="sv-SE", feed_language="sv", fallback_language="sv", attribute=None)
    assert format_price(10, context) == "10.0000"


def test_data_register_label():
    attribute = {
        "options": {"4": {"sv": "Vit", "nb": "Hvit"}},
        "value": "4",
    }
    context = TransformContext(culture="nb-NO", feed_language="nb", fallback_language="sv", attribute=attribute)
    assert data_register_label("4", context) == "Hvit"


def test_data_register_label_fallback_to_sv():
    attribute = {
        "options": {"4": {"sv": "Vit"}},
        "value": "4",
    }
    context = TransformContext(culture="nb-NO", feed_language="nb", fallback_language="sv", attribute=attribute)
    assert data_register_label("4", context) == "Vit"


def test_data_register_label_empty_prefers_fallback():
    attribute = {
        "options": {"4": {"nb": "", "sv": "Vit"}},
        "value": "4",
    }
    context = TransformContext(culture="nb-NO", feed_language="nb", fallback_language="sv", attribute=attribute)
    assert data_register_label("4", context) == "Vit"
