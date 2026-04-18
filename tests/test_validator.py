"""Validator tests — ensure schema checks behave correctly."""
from core.validator import validate, normalize_result


def test_all_fields_present_non_null():
    fields = ["author", "year", "n", "outcome"]
    result = {"author": "Smith et al. (2023)", "year": "2023", "n": "120", "outcome": "improved"}
    r = validate(result, fields)
    assert r.ok
    assert r.non_null_count == 4
    assert r.missing_fields == []


def test_missing_fields_flagged_but_ok():
    fields = ["author", "year", "n", "outcome"]
    result = {"author": "Smith et al. (2023)", "year": "2023"}
    r = validate(result, fields)
    assert r.ok  # non_null > 0 is enough at chunk level
    assert "n" in r.missing_fields
    assert "outcome" in r.missing_fields


def test_all_null_not_ok():
    fields = ["author", "year"]
    result = {"author": None, "year": None}
    r = validate(result, fields)
    assert not r.ok


def test_normalize_fills_missing_with_none():
    fields = ["a", "b", "c"]
    result = {"a": "x", "z": "extra"}  # missing b,c; extra z
    norm = normalize_result(result, fields)
    assert set(norm.keys()) == {"a", "b", "c"}
    assert norm["a"] == "x"
    assert norm["b"] is None
    assert norm["c"] is None
    assert "z" not in norm
