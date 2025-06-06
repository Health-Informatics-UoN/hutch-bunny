import pytest
from unittest.mock import Mock
from sqlalchemy import Select

from hutch_bunny.core.solvers.availability_solver import ResultModifier
from hutch_bunny.core.solvers.demographics_solver import (
    DemographicsDistributionQuerySolver,
    DemographicsRow,
)
from hutch_bunny.core.rquest_dto.query import DistributionQuery


@pytest.fixture
def mock_db_manager() -> Mock:
    """Create a mock database manager."""
    return Mock()


@pytest.fixture
def mock_query() -> Mock:
    """Create a mock query."""
    return Mock(spec=DistributionQuery, collection="test_collection")


@pytest.fixture
def solver(
    mock_db_manager: Mock, mock_query: Mock
) -> DemographicsDistributionQuerySolver:
    """Create a solver instance with mocked dependencies."""
    return DemographicsDistributionQuerySolver(mock_db_manager, mock_query)


def test_get_modifier_values_default(
    solver: DemographicsDistributionQuerySolver,
) -> None:
    """Test _get_modifier_values with default values."""
    # Arrange
    modifiers: list[ResultModifier] = []

    # Act
    low_number, rounding = solver._get_modifier_values(modifiers)

    # Assert
    assert low_number == solver.DEFAULT_LOW_NUMBER
    assert rounding == solver.DEFAULT_ROUNDING


def test_get_modifier_values_custom(
    solver: DemographicsDistributionQuerySolver,
) -> None:
    """Test _get_modifier_values with custom values."""
    # Arrange
    modifiers: list[ResultModifier] = [
        {"id": "Low Number Suppression", "threshold": 5, "nearest": None},
        {"id": "Rounding", "threshold": None, "nearest": 20},
    ]

    # Act
    low_number, rounding = solver._get_modifier_values(modifiers)

    # Assert
    assert low_number == 5
    assert rounding == 20


def test_get_modifier_values_none_values(
    solver: DemographicsDistributionQuerySolver,
) -> None:
    """Test _get_modifier_values with None values in modifiers."""
    # Arrange
    modifiers: list[ResultModifier] = [
        {"id": "Low Number Suppression", "threshold": None, "nearest": None},
        {"id": "Rounding", "threshold": None, "nearest": None},
    ]

    # Act
    low_number, rounding = solver._get_modifier_values(modifiers)

    # Assert
    assert low_number == solver.DEFAULT_LOW_NUMBER
    assert rounding == solver.DEFAULT_ROUNDING


def test_build_gender_query_with_rounding(
    solver: DemographicsDistributionQuerySolver,
) -> None:
    """Test _build_gender_query with rounding."""
    # Arrange
    rounding = 10
    low_number = 5

    # Act
    stmnt = solver._build_gender_query(rounding=rounding, low_number=low_number)

    # Assert
    assert isinstance(stmnt, Select)
    assert "round" in str(stmnt).lower()
    assert "group by" in str(stmnt).lower()


def test_build_gender_query_without_rounding(
    solver: DemographicsDistributionQuerySolver,
) -> None:
    """Test _build_gender_query without rounding."""
    # Arrange
    rounding = 0
    low_number = 5

    # Act
    stmnt = solver._build_gender_query(rounding=rounding, low_number=low_number)

    # Assert
    assert isinstance(stmnt, Select)
    assert "round" not in str(stmnt).lower()
    assert "group by" in str(stmnt).lower()


def test_build_gender_query_with_low_number(
    solver: DemographicsDistributionQuerySolver,
) -> None:
    """Test _build_gender_query with low number suppression."""
    # Arrange
    rounding = 10
    low_number = 5

    # Act
    stmnt = solver._build_gender_query(rounding=rounding, low_number=low_number)

    # Assert
    assert isinstance(stmnt, Select)
    assert "having" in str(stmnt).lower()


def test_build_alternatives_string(solver: DemographicsDistributionQuerySolver) -> None:
    """Test _build_alternatives_string."""
    # Arrange
    counts_by_gender = {8507: 40, 8532: 60}
    concept_names = {8507: "MALE", 8532: "FEMALE"}
    modifiers: list[ResultModifier] = []

    # Act
    result = solver._build_alternatives_string(
        counts_by_gender, concept_names, modifiers
    )

    # Assert
    assert result == "^MALE|40^FEMALE|60^"


def test_build_alternatives_string_with_modifiers(
    solver: DemographicsDistributionQuerySolver,
) -> None:
    """Test _build_alternatives_string with modifiers."""
    # Arrange
    counts_by_gender = {8507: 40, 8532: 60}
    concept_names = {8507: "MALE", 8532: "FEMALE"}
    modifiers: list[ResultModifier] = [{"id": "Rounding", "nearest": 10}]  # type: ignore

    # Act
    result = solver._build_alternatives_string(
        counts_by_gender, concept_names, modifiers
    )

    # Assert
    assert result == "^MALE|40^FEMALE|60^"


def test_create_demographics_rows(solver: DemographicsDistributionQuerySolver) -> None:
    """Test _create_demographics_rows."""
    # Arrange
    total_count = 100
    alternatives = "^MALE|40^FEMALE|60^"

    # Act
    rows = solver._create_demographics_rows(total_count, alternatives)
    assert len(rows) == 2
    assert isinstance(rows[0], DemographicsRow)
    assert isinstance(rows[1], DemographicsRow)

    # Check SEX row
    assert rows[0].code == "SEX"
    assert rows[0].count == total_count
    assert rows[0].alternatives == alternatives

    # Check GENOMICS row
    assert rows[1].code == "GENOMICS"
    assert rows[1].count == total_count
    assert rows[1].alternatives == f"^No|{total_count}^"


def test_demographics_row_to_dict() -> None:
    """Test DemographicsRow.to_dict method."""
    row = DemographicsRow(
        code="TEST",
        description="Test Description",
        count=100,
        alternatives="^TEST|100^",
        biobank="test_biobank",
    )

    result = row.to_dict()
    assert isinstance(result, dict)
    assert result["CODE"] == "TEST"
    assert result["DESCRIPTION"] == "Test Description"
    assert result["COUNT"] == "100"
    assert result["ALTERNATIVES"] == "^TEST|100^"
    assert result["BIOBANK"] == "test_biobank"
    assert result["CATEGORY"] == "DEMOGRAPHICS"
    assert result["DATASET"] == "person"
