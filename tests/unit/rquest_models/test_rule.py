import pytest
from hutch_bunny.core.rquest_models.rule import Rule


def test_rule_basic_initialization() -> None:
    """Test basic rule initialization with default values"""
    with pytest.raises(ValueError):
        Rule()  # type: ignore

    rule = Rule(varcat="Person")
    assert rule.value == ""
    assert rule.type_ == "TEXT"
    assert rule.time is None
    assert rule.varname == ""
    assert rule.operator == "="
    assert rule.raw_range is None
    assert rule.varcat == "Person"
    assert rule.secondary_modifier is None
    assert rule.min_value is None
    assert rule.max_value is None


def test_rule_omop_concept() -> None:
    """Test rule with OMOP concept"""
    rule = Rule(
        varname="OMOP", varcat="Person", type_="TEXT", operator="=", value="8507"
    )
    assert rule.value == "8507"
    assert rule.type_ == "TEXT"
    assert rule.varcat == "Person"
    assert rule.operator == "="
    assert rule.min_value is None
    assert rule.max_value is None


def test_rule_numeric_age_range() -> None:
    """Test rule with numeric age range"""
    rule = Rule(type_="NUM", value="18.0..65.0", varname="age=value", varcat="Person")
    assert rule.min_value == 18.0
    assert rule.max_value == 65.0
    assert rule.raw_range == "18.0..65.0"
    assert rule.value == "value"


def test_rule_numeric_with_invalid_format() -> None:
    """Test numeric rule with invalid format"""
    rule = Rule(
        type_="NUM", value="invalid_format", varname="age=value", varcat="Person"
    )
    assert rule.min_value is None
    assert rule.max_value is None
    assert rule.raw_range == "invalid_format"
    assert rule.value == "value"


def test_rule_numeric_with_invalid_values() -> None:
    """Test numeric rule with invalid numeric values in range"""
    # Test with non-numeric values in range
    rule = Rule(type_="NUM", value="abc..def", varname="age=value", varcat="Person")
    assert rule.min_value is None
    assert rule.max_value is None
    assert rule.raw_range == "abc..def"
    assert rule.value == "value"

    # Test with null values in range
    rule = Rule(type_="NUM", value="null..null", varname="age=value", varcat="Person")
    assert rule.min_value is None
    assert rule.max_value is None
    assert rule.raw_range == "null..null"
    assert rule.value == "value"

    # Test with mixed valid and invalid values
    rule = Rule(type_="NUM", value="18.0..null", varname="age=value", varcat="Person")
    assert rule.min_value == 18.0
    assert rule.max_value is None
    assert rule.raw_range == "18.0..null"
    assert rule.value == "value"

    rule = Rule(type_="NUM", value="null..65.0", varname="age=value", varcat="Person")
    assert rule.min_value is None
    assert rule.max_value == 65.0
    assert rule.raw_range == "null..65.0"
    assert rule.value == "value"


def test_rule_condition_concept() -> None:
    """Test rule with condition concept"""
    rule = Rule(
        type_="TEXT", value="some_value", varname="name=value", varcat="Condition"
    )
    assert rule.min_value is None
    assert rule.max_value is None
    assert rule.raw_range is None
    assert rule.value == "some_value"


def test_rule_varname_parsing() -> None:
    """Test varname parsing with and without equals sign"""
    rule = Rule(type_="NUM", value="18..65", varname="age=value", varcat="Person")
    assert rule.value == "value"

    rule = Rule(type_="NUM", value="18..65", varname="age", varcat="Person")
    assert rule.value == ""


def test_rule_secondary_modifier() -> None:
    """Test rule with secondary modifier"""
    rule = Rule(secondary_modifier=[1, 2, 3], varcat="Condition")
    assert rule.secondary_modifier == [1, 2, 3]

    rule = Rule(secondary_modifier=None, varcat="Condition")
    assert rule.secondary_modifier is None


def test_rule_invalid_type() -> None:
    """Test rule with invalid type"""
    with pytest.raises(ValueError):
        Rule(type_="INVALID", varcat="Person")  # type: ignore


def test_rule_invalid_operator() -> None:
    """Test rule with invalid operator"""
    with pytest.raises(ValueError):
        Rule(operator=">", varcat="Person")  # type: ignore


def test_rule_invalid_varcat() -> None:
    """Test rule with invalid varcat"""
    with pytest.raises(ValueError):
        Rule(varcat="InvalidCategory")  # type: ignore


def test_rule_geo_radius_parses_value() -> None:
    """GEO_RADIUS type parses lat|lon|radius from value into typed fields."""
    rule = Rule(
        varname="OMOP",
        varcat="Location",
        type_="GEO_RADIUS",
        value="51.5074|-0.1278|5000",
    )
    assert rule.center_lat == pytest.approx(51.5074)
    assert rule.center_lon == pytest.approx(-0.1278)
    assert rule.geo_radius_meters == pytest.approx(5000.0)
    assert rule.value == ""  # cleared so concept lookups are unaffected


def test_rule_geo_radius_clears_value() -> None:
    """GEO_RADIUS rule always clears the value field after parsing."""
    rule = Rule(varcat="Location", type_="GEO_RADIUS", value="0.0|0.0|100")
    assert rule.value == ""


def test_rule_geo_radius_invalid_value_leaves_fields_none() -> None:
    """Malformed GEO_RADIUS value leaves geo fields as None rather than raising."""
    rule = Rule(varcat="Location", type_="GEO_RADIUS", value="not|valid")
    assert rule.center_lat is None
    assert rule.center_lon is None
    assert rule.geo_radius_meters is None


def test_rule_text_location_unaffected() -> None:
    """TEXT Location rules still work and do not populate geo fields."""
    rule = Rule(varcat="Location", type_="TEXT", value="", secondary_modifier=["GBR"])
    assert rule.center_lat is None
    assert rule.center_lon is None
    assert rule.geo_radius_meters is None
    assert rule.secondary_modifier == ["GBR"]
