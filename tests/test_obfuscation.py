from hutch_bunny.core.obfuscation import (
    apply_filters,
    low_number_suppression,
    rounding,
)


def test_low_number_suppression():
    assert low_number_suppression(99, threshold=100) == 0
    assert low_number_suppression(100, threshold=100) == 0
    assert low_number_suppression(101, threshold=100) == 101


def test_rounding():
    assert rounding(123, nearest=100) == 100
    assert rounding(123, nearest=10) == 120
    assert rounding(123, nearest=1) == 123


def test_apply_filters():
    # Test rounding only
    filters = [{"id": "Rounding", "nearest": 100}]
    assert apply_filters(123, filters=filters) == 100

    # Test low number suppression only
    filters = [{"id": "Low Number Suppression", "threshold": 100}]
    assert apply_filters(123, filters=filters) == 123

    # Test both filters
    filters = [
        {"id": "Low Number Suppression", "threshold": 100},
        {"id": "Rounding", "nearest": 100},
    ]
    assert apply_filters(123, filters=filters) == 100


def test_apply_filters_leak():
    # Test that putting the rounding filter first can leak the low number suppression filter
    filters = [
        {"id": "Rounding", "nearest": 100},
        {"id": "Low Number Suppression", "threshold": 70},
    ]
    assert apply_filters(60, filters=filters) == 100
