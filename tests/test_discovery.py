from pathlib import Path

from src.discovery import discover_mapping
from src.mapping_loader import load_mapping


class StubFeedClient:
    def __init__(self, product):
        self.product = product

    def fetch_products(self, export_from, product_no=None, limit=None):
        return [self.product]


class StubJetshopClient:
    def dyn_get(self, article_numbers, cultures):
        return {"new_dyn_field": {"sv-SE": "value"}}


def test_discover_mapping(tmp_path):
    product = {
        "identifier": {"productNo": "Pelle-1092-10"},
        "attributes": [
            {"importCode": "unmapped_attr", "dataType": "UNI_TEXT", "value": "value"},
        ],
        "texts": [
            {"importCode": "unmapped_text", "value": {"sv": "value"}, "maxLength": 100},
        ],
    }

    mapping_path = Path(__file__).resolve().parents[1] / "mappings" / "mapping.yaml"
    mapping = load_mapping(mapping_path)

    output_path = tmp_path / "mapping_suggestions.yaml"
    suggestions = discover_mapping(
        StubFeedClient(product),
        StubJetshopClient(),
        mapping,
        "2025-01-01T00:00:00Z",
        "Pelle-1092-10",
        output_path=str(output_path),
    )

    assert output_path.exists()
    assert len(suggestions["unmapped_attributes"]) == 1
    assert len(suggestions["unmapped_texts"]) == 1
    assert len(suggestions["unmapped_dynamic_fields"]) == 1
