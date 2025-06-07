from hutch_bunny.core.rquest_models.rule import Rule


def test_rule_basic_initialization() -> None:
    """Test basic rule initialization with default values"""
    rule = Rule()
    assert rule.value is None
    assert rule.type_ == ""
    assert rule.time is None
    assert rule.varname == ""
    assert rule.operator == ""
    assert rule.raw_range == ""
    assert rule.varcat is None
    assert rule.secondary_modifier is None
    assert rule.min_value is None
    assert rule.max_value is None


def test_rule_numeric_initialization() -> None:
    """Test rule initialization with numeric type and range"""
    rule = Rule(type_="NUM", value="10.5..20.5", varname="age=value")
    assert rule.min_value == 10.5
    assert rule.max_value == 20.5
    assert rule.raw_range == "10.5..20.5"
    assert rule.value == "value"


def test_rule_numeric_with_null_bounds() -> None:
    """Test numeric rule with null bounds"""
    rule = Rule(type_="NUM", value="null..20.5", varname="age=value")
    assert rule.min_value is None
    assert rule.max_value == 20.5

    rule = Rule(type_="NUM", value="10.5..null", varname="age=value")
    assert rule.min_value == 10.5
    assert rule.max_value is None


def test_rule_numeric_with_invalid_bounds() -> None:
    """Test numeric rule with invalid bounds"""
    rule = Rule(type_="NUM", value="invalid..20.5", varname="age=value")
    assert rule.min_value is None
    assert rule.max_value == 20.5

    rule = Rule(type_="NUM", value="10.5..invalid", varname="age=value")
    assert rule.min_value == 10.5
    assert rule.max_value is None


def test_rule_numeric_with_invalid_format() -> None:
    """Test numeric rule with invalid format"""
    rule = Rule(type_="NUM", value="invalid_format", varname="age=value")
    assert rule.min_value is None
    assert rule.max_value is None
    assert rule.raw_range == "invalid_format"
    assert rule.value == "value"


def test_rule_non_numeric_type() -> None:
    """Test rule with non-numeric type"""
    rule = Rule(type_="TEXT", value="some_value", varname="name=value")
    assert rule.min_value is None
    assert rule.max_value is None
    assert rule.raw_range == ""
    assert rule.value == "some_value"


def test_rule_varname_parsing() -> None:
    """Test varname parsing with and without equals sign"""
    rule = Rule(type_="NUM", value="10.5..20.5", varname="age=value")
    assert rule.value == "value"

    rule = Rule(type_="NUM", value="10.5..20.5", varname="age")
    assert rule.value == "10.5..20.5"


def test_rule_secondary_modifier() -> None:
    """Test rule with secondary modifier"""
    rule = Rule(secondary_modifier=["1", "2", "3"])
    assert rule.secondary_modifier == ["1", "2", "3"]

    rule = Rule(secondary_modifier=None)
    assert rule.secondary_modifier is None
