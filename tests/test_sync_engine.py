import logging
from pathlib import Path

from src.jetshop_client import NIL_VALUE
from src.mapping_loader import load_mapping
from src.state_store import StateStore
from src.sync_engine import SyncEngine


def build_sample_product():
    return {
        "identifier": {"productNo": "Pelle-1092-10"},
        "attributes": [
            {"importCode": "monitor_disp", "dataType": "FLOAT", "value": 10.0},
            {"importCode": "monitor_GTIN", "dataType": "UNI_TEXT", "value": "123"},
            {"importCode": "b2c_price_se", "dataType": "FLOAT", "value": 100.0},
            {"importCode": "b2c_price_no", "dataType": "FLOAT", "value": 110.0},
            {"importCode": "b2c_price_b2b", "dataType": "FLOAT", "value": 120.0},
            {"importCode": "se_show_mp", "dataType": "BOOLEAN", "value": True},
            {"importCode": "no_show_mp", "dataType": "BOOLEAN", "value": True},
            {"importCode": "b2b_show_mp", "dataType": "BOOLEAN", "value": True},
            {"importCode": "monitor_deliverydate", "dataType": "DATE", "value": "2025-01-02T00:00:00"},
            {
                "importCode": "jetshop_category_mp",
                "dataType": "DATA_REGISTER_MULTI",
                "value": ["150", "151"],
            },
            {"importCode": "atr_height", "dataType": "FLOAT", "value": 63},
            {
                "importCode": "atr_colour",
                "dataType": "DATA_REGISTER",
                "options": {"4": {"sv": "Vit", "nb": "Hvit"}},
                "value": "4",
            },
            {"importCode": "atr_grouping", "dataType": "UNI_TEXT", "value": "Group"},
            {"importCode": "spec_cat", "dataType": "UNI_TEXT", "value": {"sv": "SpecCat", "nb": "SpecCatNb"}},
            {
                "importCode": "spec_subcat",
                "dataType": "UNI_TEXT",
                "value": {"sv": "SpecSub", "nb": "SpecSubNb"},
            },
        ],
        "texts": [
            {"importCode": "name_1", "value": {"sv": "Nigella", "nb": "Jomfru"}, "maxLength": 200},
            {"importCode": "name_2", "value": {"sv": "", "nb": ""}, "maxLength": 200},
            {"importCode": "productinfoshort", "value": {"sv": "Short", "nb": "Short nb"}, "maxLength": 300},
            {"importCode": "productinfolong", "value": {"sv": "L1\nL2", "nb": "N1\nN2"}, "maxLength": 2000},
        ],
    }


class StubFeedClient:
    def __init__(self, products):
        self.products = products

    def fetch_products(self, export_from, product_no=None, limit=None):
        if product_no:
            return [p for p in self.products if p["identifier"]["productNo"] == product_no]
        return self.products[:limit] if limit else self.products


class StubJetshopClient:
    def __init__(self, raise_on_get=False, dyn_failures=None):
        self.raise_on_get = raise_on_get
        self.dyn_failures = dyn_failures or []
        self.add_update_calls = 0
        self.dyn_save_calls = 0
        self.price_list_calls = 0
        self.price_list_inputs = []
        self.add_update_payloads = []

    def product_get(self, culture, article_number):
        if self.raise_on_get:
            raise RuntimeError("Jetshop read failed")
        return {}

    def dyn_get(self, article_numbers, cultures):
        if self.raise_on_get:
            raise RuntimeError("Jetshop read failed")
        return {}

    def product_add_update(self, product_data_list):
        self.add_update_calls += 1
        self.add_update_payloads.append(product_data_list)
        return []

    def dyn_save(self, inputs):
        self.dyn_save_calls += 1
        return self.dyn_failures

    def price_list_update(self, inputs):
        self.price_list_calls += 1
        self.price_list_inputs = inputs


def test_sync_engine_dry_run_writes_diff(tmp_path, monkeypatch):
    mapping_path = Path(__file__).resolve().parents[1] / "mappings" / "mapping.yaml"
    mapping = load_mapping(mapping_path)

    feed_client = StubFeedClient([build_sample_product()])
    jetshop_client = StubJetshopClient()
    logger = logging.getLogger("test_sync_engine")
    logger.addHandler(logging.NullHandler())
    state_store = StateStore(tmp_path / "state" / "last_run.json")

    monkeypatch.chdir(tmp_path)

    engine = SyncEngine(feed_client, jetshop_client, mapping, logger, state_store)
    report = engine.sync("2025-01-01T00:00:00Z", "Pelle-1092-10", None, True)

    diff_path = tmp_path / "diffs" / "Pelle-1092-10.json"
    assert diff_path.exists()
    assert report["counts"]["processed"] == 1
    assert jetshop_client.add_update_calls == 0
    assert jetshop_client.dyn_save_calls == 0
    assert jetshop_client.price_list_calls == 0


def test_sync_engine_handles_read_failure(tmp_path, monkeypatch):
    mapping_path = Path(__file__).resolve().parents[1] / "mappings" / "mapping.yaml"
    mapping = load_mapping(mapping_path)

    feed_client = StubFeedClient([build_sample_product()])
    jetshop_client = StubJetshopClient(raise_on_get=True)
    logger = logging.getLogger("test_sync_engine_read_fail")
    logger.addHandler(logging.NullHandler())
    state_store = StateStore(tmp_path / "state" / "last_run.json")

    monkeypatch.chdir(tmp_path)

    engine = SyncEngine(feed_client, jetshop_client, mapping, logger, state_store)
    report = engine.sync("2025-01-01T00:00:00Z", "Pelle-1092-10", None, False)

    assert report["counts"]["failed"] == 1


def test_sync_engine_ignores_missing_dynamic_fields(tmp_path, monkeypatch):
    mapping_path = Path(__file__).resolve().parents[1] / "mappings" / "mapping.yaml"
    mapping = load_mapping(mapping_path)

    feed_client = StubFeedClient([build_sample_product()])

    class Result:
        def __init__(self, key, success, message):
            self.key = key
            self.success = success
            self.message = message

    dyn_failures = [
        Result("atr_height", False, "No dynamic field, connected to product, found with key."),
        Result("atr_colour", False, "No dynamic field, connected to product, found with key."),
    ]

    jetshop_client = StubJetshopClient(dyn_failures=dyn_failures)
    logger = logging.getLogger("test_sync_engine_dyn_missing")
    logger.addHandler(logging.NullHandler())
    state_store = StateStore(tmp_path / "state" / "last_run.json")

    monkeypatch.chdir(tmp_path)

    engine = SyncEngine(feed_client, jetshop_client, mapping, logger, state_store)
    report = engine.sync("2025-01-01T00:00:00Z", "Pelle-1092-10", None, False)

    assert report["counts"]["failed"] == 0


def test_sync_engine_clears_discount_on_missing(tmp_path, monkeypatch):
    mapping_path = Path(__file__).resolve().parents[1] / "mappings" / "mapping.yaml"
    mapping = load_mapping(mapping_path)

    feed_client = StubFeedClient([build_sample_product()])
    jetshop_client = StubJetshopClient()
    logger = logging.getLogger("test_sync_engine_discount_clear")
    logger.addHandler(logging.NullHandler())
    state_store = StateStore(tmp_path / "state" / "last_run.json")

    monkeypatch.chdir(tmp_path)

    engine = SyncEngine(feed_client, jetshop_client, mapping, logger, state_store)
    report = engine.sync("2025-01-01T00:00:00Z", "Pelle-1092-10", None, False)

    assert report["counts"]["failed"] == 0
    assert jetshop_client.price_list_inputs
    for item in jetshop_client.price_list_inputs:
        assert item.get("DiscountedPriceIncVat") == -1
        assert item.get("HideProduct") is False


def test_sync_engine_clears_discount_on_zero(tmp_path, monkeypatch):
    mapping_path = Path(__file__).resolve().parents[1] / "mappings" / "mapping.yaml"
    mapping = load_mapping(mapping_path)

    product = build_sample_product()
    product["attributes"].append(
        {"importCode": "b2c_disc_price_mp_se", "dataType": "FLOAT", "value": 0.0}
    )
    product["attributes"].append(
        {"importCode": "b2c_disc_price_mp_no", "dataType": "FLOAT", "value": "0"}
    )
    product["attributes"].append(
        {"importCode": "b2c_disc_price_mp_b2b", "dataType": "FLOAT", "value": 0}
    )

    feed_client = StubFeedClient([product])
    jetshop_client = StubJetshopClient()
    logger = logging.getLogger("test_sync_engine_discount_zero")
    logger.addHandler(logging.NullHandler())
    state_store = StateStore(tmp_path / "state" / "last_run.json")

    monkeypatch.chdir(tmp_path)

    engine = SyncEngine(feed_client, jetshop_client, mapping, logger, state_store)
    report = engine.sync("2025-01-01T00:00:00Z", "Pelle-1092-10", None, False)

    assert report["counts"]["failed"] == 0
    assert jetshop_client.price_list_inputs
    for item in jetshop_client.price_list_inputs:
        assert item.get("DiscountedPriceIncVat") == -1


def test_sync_engine_clears_categories_before_update(tmp_path, monkeypatch):
    mapping_path = Path(__file__).resolve().parents[1] / "mappings" / "mapping.yaml"
    mapping = load_mapping(mapping_path)

    feed_client = StubFeedClient([build_sample_product()])

    class CategoryJetshopClient(StubJetshopClient):
        def product_get(self, culture, article_number):
            return {"ProductInCategories": ["150", "151", "999"]}

    jetshop_client = CategoryJetshopClient()
    logger = logging.getLogger("test_sync_engine_category_reset")
    logger.addHandler(logging.NullHandler())
    state_store = StateStore(tmp_path / "state" / "last_run.json")

    monkeypatch.chdir(tmp_path)

    engine = SyncEngine(feed_client, jetshop_client, mapping, logger, state_store)
    report = engine.sync("2025-01-01T00:00:00Z", "Pelle-1092-10", None, False)

    assert report["counts"]["failed"] == 0
    assert jetshop_client.add_update_calls == 1
    update_payloads = jetshop_client.add_update_payloads[0]
    for payload in update_payloads:
        categories = payload.get("ProductInCategories", [])
        delete_entries = [
            item
            for item in categories
            if isinstance(item, dict) and item.get("ProductInCategoryState") == "DeleteConnection"
        ]
        assert delete_entries
        assert delete_entries[0]["CategoryId"] == "999"
