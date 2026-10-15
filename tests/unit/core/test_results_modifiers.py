from hutch_bunny.core.results_modifiers import (
    DEFAULT_LOW_NUMBER_THRESHOLD,
    DEFAULT_ROUNDING_TARGET,
    ResultModifier,
    get_modifier_value,
)


def test_get_modifier_value_returns_value() -> None:
    modifiers: list[ResultModifier] = [
        {"id": "Low Number Suppression", "threshold": 5},
        {"id": "Rounding", "nearest": 100},
    ]

    assert get_modifier_value(modifiers, "Low Number Suppression", "threshold") == 5
    assert get_modifier_value(modifiers, "Rounding", "nearest") == 100


def test_get_modifier_value_keeps_zero() -> None:
    """A value of 0 turns the modifier off, so must not be replaced by the default."""
    modifiers: list[ResultModifier] = [{"id": "Rounding", "nearest": 0}]

    assert get_modifier_value(modifiers, "Rounding", "nearest") == 0


def test_get_modifier_value_uses_default() -> None:
    test_cases: list[list[ResultModifier]] = [
        [],  # modifier missing
        [{"id": "Rounding"}],  # key missing
        [{"id": "Rounding", "nearest": None}],  # value is None
        [{"id": "Low Number Suppression", "threshold": 5}],  # only a different modifier
    ]

    for modifiers in test_cases:
        assert get_modifier_value(modifiers, "Rounding", "nearest") == DEFAULT_ROUNDING_TARGET


def test_get_modifier_value_uses_default_for_each_modifier() -> None:
    modifiers: list[ResultModifier] = []

    assert get_modifier_value(modifiers, "Low Number Suppression", "threshold") == DEFAULT_LOW_NUMBER_THRESHOLD
    assert get_modifier_value(modifiers, "Rounding", "nearest") == DEFAULT_ROUNDING_TARGET
