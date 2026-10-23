import json
from typing import Literal, NotRequired, TypedDict


class ResultModifier(TypedDict):
    """A results modifier, e.g. low number suppression or rounding."""

    id: str
    threshold: NotRequired[int | None]
    nearest: NotRequired[int | None]


ModifierKey = Literal["threshold", "nearest"]

# Used when a modifier value is not supplied
DEFAULT_LOW_NUMBER_THRESHOLD = 10
DEFAULT_ROUNDING_TARGET = 10

DEFAULT_MODIFIER_VALUES: dict[ModifierKey, int] = {
    "threshold": DEFAULT_LOW_NUMBER_THRESHOLD,
    "nearest": DEFAULT_ROUNDING_TARGET,
}


def get_modifier_value(
    modifiers: list[ResultModifier],
    modifier_id: str,
    key: ModifierKey,
) -> int:
    """Get a value from the first modifier with the given id.

    Args:
        modifiers (list[ResultModifier]): The results modifiers.
        modifier_id (str): The modifier id, e.g. "Rounding".
        key (ModifierKey): The value to read, e.g. "nearest".

    Returns:
        int: The modifier value, or the default for that key if the modifier,
        key or value is missing. A value of 0 is kept.
    """
    default = DEFAULT_MODIFIER_VALUES[key]
    for item in modifiers:
        if item["id"] == modifier_id:
            value = item.get(key)
            return value if value is not None else default
    return default


def results_modifiers(
    low_number_suppression_threshold: int,
    rounding_target: int,
) -> list[ResultModifier]:
    return [
        {
            "id": "Low Number Suppression",
            "threshold": low_number_suppression_threshold,
        },
        {
            "id": "Rounding",
            "nearest": rounding_target,
        },
    ]


def get_results_modifiers_from_str(params: str) -> list[ResultModifier]:
    """Deserialise a JSON list containing results modifiers

    Args:
        params (str):
        The JSON string containing list of parameter objects for results modifiers

    Raises:
        ValueError: The parsed string does not produce a list

    Returns:
        list: The list of parameter dicts of results modifiers
    """

    deserialised_params: list[ResultModifier] = json.loads(params)
    if not isinstance(deserialised_params, list):
        raise ValueError(
            f"{get_results_modifiers_from_str.__name__} requires a JSON list"
        )
    return deserialised_params
