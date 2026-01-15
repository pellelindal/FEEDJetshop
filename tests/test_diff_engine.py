from src.diff_engine import diff_categories, diff_dynamic_fields, diff_product_data, diff_stock


def test_diff_product_data():
    current = {"Name": "Old", "Price": "10.0000"}
    desired = {"Name": "New", "Price": "10.0000"}
    diffs = diff_product_data(current, desired, "sv-SE")
    assert len(diffs) == 1
    assert diffs[0].target_field == "Name"


def test_diff_categories():
    diffs = diff_categories(["1", "2"], ["2", "3"], "sv-SE")
    assert len(diffs) == 1
    assert diffs[0].target_field == "ProductInCategories"


def test_diff_stock():
    current = {"NewStockCount": 5}
    desired = {"NewStockCount": 6}
    diffs = diff_stock(current, desired, "sv-SE")
    assert len(diffs) == 1


def test_diff_dynamic_fields():
    current = {"atr_colour": {"sv-SE": "Vit"}}
    desired = {"atr_colour": {"sv-SE": "Hvit"}}
    diffs = diff_dynamic_fields(current, desired)
    assert len(diffs) == 1
    assert diffs[0].target_field == "atr_colour"
