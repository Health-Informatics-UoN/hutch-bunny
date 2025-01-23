import json
from typing import Union


def get_results_modifiers_from_str(params: str) -> list:
    """Deserialise a JSON list containing results modifiers

    Args:
        params (str):
        The JSON string containing list of parameter objects for results modifiers

    Raises:
        ValueError: The parsed string does not produce a list

    Returns:
        list: The list of parameter dicts of results modifiers
    """
    deserialised_params = json.loads(params)
    if not isinstance(deserialised_params, list):
        raise ValueError(
            f"{get_results_modifiers_from_str.__name__} requires a JSON list"
        )
    return deserialised_params


def low_number_suppression(
    value: Union[int, float], threshold: int = 10
) -> Union[int, float]:
    """Suppress values that fall below a given threshold.

    Args:
        value (Union[int, float]): The value to evaluate.
        threshold (int): The threshold to beat.

    Returns:
        Union[int, float]: `value` if `value` > `threshold` else `0`.

    Examples:
        >>> low_number_suppression(99, threshold=100)
        0
        >>> low_number_suppression(200, threshold=100)
        200
    """
    return value if value > threshold else 0


def rounding(value: Union[int, float], nearest: int = 10) -> int:
    """Round the value to the nearest base number, e.g. 10.

    Args:
        value (Union[int, float]): The value to be rounded
        nearest (int, optional): Round value to this base. Defaults to 10.

    Returns:
        int: The value rounded to the specified nearest interval.

    Examples:
        >>> rounding(145, nearest=100)
        100
        >>> rounding(160, nearest=100)
        200
    """
    return nearest * round(value / nearest)


def apply_filters(value: Union[int, float], filters: list) -> Union[int, float]:
    """Iterate over a list of filters and apply them to the supplied value.

    Args:
        value (Union[int, float]): The value to be filtered.
        filters (list): The filters applied to the value.

    Returns:
        Union[int, float]: The filtered value.
    """
    actions = {"Low Number Suppression": low_number_suppression, "Rounding": rounding}
    result = value
    for f in filters:
        if action := actions.get(f.pop("id", None)):
            result = action(result, **f)
            if result == 0:
                break  # don't apply any more filters
    return result
