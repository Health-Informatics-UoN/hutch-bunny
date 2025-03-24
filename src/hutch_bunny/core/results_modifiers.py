import json


def results_modifiers(
    low_number_suppression_threshold: int,
    rounding_target: int,
) -> list[dict[str, str | int]]:
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


def get_results_modifiers_from_str(params: str) -> list[dict]:
    """Deserialise a JSON list containing results modifiers

    Args:
        params (str):
        The JSON string containing list of parameter objects for results modifiers

    Raises:
        ValueError: The parsed string does not produce a list

    Returns:
        list: The list of parameter dicts of results modifiers
    """

    deserialised_params: list[dict] = json.loads(params)
    if not isinstance(deserialised_params, list):
        raise ValueError(
            f"{get_results_modifiers_from_str.__name__} requires a JSON list"
        )
    return deserialised_params
