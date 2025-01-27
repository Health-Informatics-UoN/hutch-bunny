from hutch_bunny.core.obfuscation import (
    apply_filters,
    low_number_suppression,
    rounding,
)


def test_low_number_suppression():
    # Test that the threshold is applied
    assert low_number_suppression(99, threshold=100) == 0
    assert low_number_suppression(100, threshold=100) == 0
    assert low_number_suppression(101, threshold=100) == 101

    # Test that the threshold can be set to 0
    assert low_number_suppression(1, threshold=0) == 1

    # Test negative threshold is ignored
    assert low_number_suppression(1, threshold=-5) == 1


def test_rounding():
    # Test default nearest
    assert rounding(9) == 10

    # Test rounding is applied
    assert rounding(123, nearest=100) == 100
    assert rounding(123, nearest=10) == 120
    assert rounding(123, nearest=1) == 123

    # Test rounding is applied the boundary
    assert rounding(150, nearest=100) == 200


def test_apply_filters_rounding():
    # Test rounding only
    filters = [{"id": "Rounding", "nearest": 100}]
    assert apply_filters(123, filters=filters) == 100


def test_apply_filters_low_number_suppression():
    # Test low number suppression only
    filters = [{"id": "Low Number Suppression", "threshold": 100}]
    assert apply_filters(123, filters=filters) == 123


def test_apply_filters_combined():
    # Test both filters
    filters = [
        {"id": "Low Number Suppression", "threshold": 100},
        {"id": "Rounding", "nearest": 100},
    ]
    assert apply_filters(123, filters=filters) == 100


def test_apply_filters_combined_leak():
    # Test that putting the rounding filter first can leak the low number suppression filter
    filters = [
        {"id": "Rounding", "nearest": 100},
        {"id": "Low Number Suppression", "threshold": 70},
    ]
    assert apply_filters(60, filters=filters) == 100


def test_apply_filters_combined_empty_filter():
    # Test that an empty filter list returns the original value
    assert apply_filters(9, []) == 9
