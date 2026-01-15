import pytest

from src.validator import ValidationError, coerce_value, validate_constraints


def test_coerce_int_strict_raises():
    with pytest.raises(ValidationError):
        coerce_value("abc", "int", "strict")


def test_coerce_int_coerce():
    assert coerce_value("10", "int", "coerce") == 10


def test_coerce_list_items():
    assert coerce_value(["1", "2"], "list", "coerce", item_type="int") == [1, 2]


def test_validate_constraints_max_length():
    with pytest.raises(ValidationError):
        validate_constraints("abcd", {"max_length": 2}, "field")
