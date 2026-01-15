from pathlib import Path

from src.mapping_loader import load_mapping
from src.sync_engine import _select_localized


def _load_mapping():
    mapping_path = Path(__file__).resolve().parents[1] / "mappings" / "mapping.yaml"
    return load_mapping(mapping_path)


def test_select_localized_nb_prefers_nb():
    mapping = _load_mapping()
    value = {"nb": "Norsk", "sv": "Svenska"}
    assert _select_localized(value, mapping, "nb-NO", None) == "Norsk"


def test_select_localized_nb_falls_back_to_sv():
    mapping = _load_mapping()
    value = {"sv": "Svenska"}
    assert _select_localized(value, mapping, "nb-NO", None) == "Svenska"


def test_select_localized_empty_nb_falls_back_to_sv():
    mapping = _load_mapping()
    value = {"nb": "", "sv": "Svenska"}
    assert _select_localized(value, mapping, "nb-NO", None) == "Svenska"


def test_select_localized_uses_culture_key_when_language_missing():
    mapping = _load_mapping()
    value = {"nb-NO": "Norsk", "sv-SE": "Svenska"}
    assert _select_localized(value, mapping, "nb-NO", None) == "Norsk"
