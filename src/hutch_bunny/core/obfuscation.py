from copy import deepcopy


def low_number_suppression(value: int | float, threshold: int = 10) -> int | float:
    """Suppress values that fall below a given threshold.

    Args:
        value (int | float): The value to evaluate.
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


def rounding(value: int | float, nearest: int = 10) -> int:
    """Round the value to the nearest base number, e.g. 10.

    Args:
        value (int | float): The value to be rounded
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


def apply_filters(value: int | float, filters: list) -> int | float:
    """Iterate over a list of filters and apply them to the supplied value.

    Makes a deep copy of the filters list to avoid mutating the original list.

    Args:
        value (int | float): The value to be filtered.
        filters (list): The filters applied to the value.

    Returns:
        int | float: The filtered value.
    """

    actions = {"Low Number Suppression": low_number_suppression, "Rounding": rounding}
    result = value
    filters_copy = deepcopy(filters)
    for f in filters_copy:
        if action := actions.get(f.pop("id", None)):
            result = action(result, **f)
            if result == 0:
                break  # don't apply any more filters
    return result
