import pytest
from datetime import datetime
from dateutil.relativedelta import relativedelta
from unittest.mock import Mock, patch
from sqlalchemy import func, text, Column, Date, Table, MetaData
from sqlalchemy.sql import CompoundSelect
from sqlalchemy.sql.elements import literal_column
from sqlalchemy.dialects import postgresql

from hutch_bunny.core.solvers.rule_query_builders import SQLDialectHandler, OMOPRuleQueryBuilder, PersonConstraintBuilder


class TestSQLDialectHandler:

    def test_get_year_difference_unsupported_dialect(self) -> None:
        """Test that an unsupported dialect raises NotImplementedError."""
        metadata = MetaData()
        test_table = Table(
            "test", metadata,
            Column("start_date", Date),
            Column("birth_date", Date),
        )

        # Mock engine with unsupported dialect name
        engine = Mock()
        engine.dialect.name = "mysql"  

        start_date = test_table.c.start_date
        birth_date = test_table.c.birth_date

        with pytest.raises(NotImplementedError, match="Unsupported database dialect"):
            SQLDialectHandler.get_year_difference(engine, start_date, birth_date)

    def test_get_year_difference_postgresql(self) -> None:
        """Test PostgreSQL dialect returns correct function."""
        engine = Mock()
        engine.dialect.name = "postgresql"

        metadata = MetaData()
        test_table = Table(
            "test", metadata,
            Column("start_date", Date),
            Column("year_of_birth", Date),
        )

        start_date = test_table.c.start_date
        year_of_birth = test_table.c.year_of_birth

        result = SQLDialectHandler.get_year_difference(engine, start_date, year_of_birth)

        assert str(result) == str(
            func.date_part("year", start_date) - year_of_birth
        )

    def test_get_year_difference_mssql(self) -> None:
        """Test MSSQL dialect returns correct function."""
        engine = Mock()
        engine.dialect.name = "mssql"

        metadata = MetaData()
        test_table = Table(
            "test", metadata,
            Column("start_date", Date),
            Column("year_of_birth", Date),
        )

        start_date = test_table.c.start_date
        year_of_birth = test_table.c.year_of_birth

        result = SQLDialectHandler.get_year_difference(engine, start_date, year_of_birth)

        assert str(result) == str(
            func.DATEPART(text("year"), start_date) - year_of_birth
        )

    def test_get_year_difference_with_actual_column_elements(self) -> None:
        """Test with actual SQLAlchemy column elements."""
        from sqlalchemy import Column, Date, Integer, create_engine
        from sqlalchemy.orm import declarative_base

        Base = declarative_base()

        class TestTable(Base):
            __tablename__ = 'test'
            id = Column(Integer, primary_key=True)
            start_date = Column(Date)
            birth_date = Column(Date)

        # Test with PostgreSQL
        pg_engine = create_engine("postgresql+psycopg://user:pass@localhost/db")
        result = SQLDialectHandler.get_year_difference(
            pg_engine,
            TestTable.start_date,
            TestTable.birth_date
        )

        # Verify it produces valid SQL when compiled
        compiled = str(result.compile(
            dialect=postgresql.dialect(), 
            compile_kwargs={"literal_binds": True}
        ))
        assert "date_part" in compiled
        assert "year" in compiled


class TestOMOPRuleQueryBuilder():
    
    def test_add_concept_constraint(self) -> None:
        mock_db_manager = Mock()
        builder = OMOPRuleQueryBuilder(mock_db_manager)

        concept_id = 111
        builder.add_concept_constraint(concept_id)

        compiled = builder.condition_query.compile(compile_kwargs={"literal_binds": True})
        sql_str = str(compiled)

        assert "WHERE condition_occurrence.condition_concept_id = 111" in sql_str

    @patch("hutch_bunny.core.solvers.rule_query_builders.SQLDialectHandler.get_year_difference")
    def test_add_age_constraint(self, mock_get_year_diff: Mock) -> None:
        mock_get_year_diff.return_value = literal_column("25")

        mock_db_manager = Mock()
        builder = OMOPRuleQueryBuilder(mock_db_manager)

        builder.add_age_constraint("20", "")  # age > 20

        compiled = builder.condition_query.compile(compile_kwargs={"literal_binds": True})
        sql_str = str(compiled)

        assert "25 > 20" in sql_str

    def test_add_temporal_constraint_only_left_constraint_present(self) -> None: 
        left_time_value = "1"
        right_time_value = ""

        fixed_now = datetime(2025, 8, 7, 12, 0, 0)

        time_to_use = int(left_time_value) * -1
        relative_date = fixed_now + relativedelta(months=time_to_use)

        expected_date_str = relative_date.strftime("%Y-%m-%d %H:%M:%S")
        expected_sql_fragment = f"condition_occurrence.condition_start_date <= '{expected_date_str}'"

        with patch("hutch_bunny.core.solvers.rule_query_builders.datetime") as mock_datetime: 
            mock_datetime.now.return_value = fixed_now

            mock_db_manager = Mock()
            builder = OMOPRuleQueryBuilder(mock_db_manager)

            builder.add_temporal_constraint(left_time_value, right_time_value)

            compiled = builder.condition_query.compile(
                compile_kwargs={"literal_binds": True}
            )
            sql_str = str(compiled)

            assert expected_sql_fragment in sql_str
    
    def test_add_temporal_constraint_only_right_constraint_present(self) -> None: 
        left_time_value = ""
        right_time_value = "1"

        fixed_now = datetime(2025, 8, 7, 12, 0, 0)

        time_to_use = int(right_time_value) * -1
        relative_date = fixed_now + relativedelta(months=time_to_use)

        expected_date_str = relative_date.strftime("%Y-%m-%d %H:%M:%S")
        expected_sql_fragment = f"condition_occurrence.condition_start_date >= '{expected_date_str}'"

        with patch("hutch_bunny.core.solvers.rule_query_builders.datetime") as mock_datetime: 
            mock_datetime.now.return_value = fixed_now

            mock_db_manager = Mock()
            builder = OMOPRuleQueryBuilder(mock_db_manager)

            builder.add_temporal_constraint(left_time_value, right_time_value)

            compiled = builder.condition_query.compile(
                compile_kwargs={"literal_binds": True}
            )
            sql_str = str(compiled)

            assert expected_sql_fragment in sql_str

    def test_add_temporal_constraint_no_input(self) -> None: 
        left_time_value = ""
        right_time_value = ""

        mock_db_manager = Mock()
        builder = OMOPRuleQueryBuilder(mock_db_manager)

        with pytest.raises(ValueError): 
            builder.add_temporal_constraint(left_time_value, right_time_value)
    
    def test_add_temporal_constraint_both_inputs(self) -> None: 
        left_time_value = "1"
        right_time_value = "2"

        mock_db_manager = Mock()
        builder = OMOPRuleQueryBuilder(mock_db_manager)

        with pytest.raises(ValueError): 
            builder.add_temporal_constraint(left_time_value, right_time_value)

    def test_add_numeric_range_with_no_input(self) -> None: 
        mock_db_manager = Mock()
        builder = OMOPRuleQueryBuilder(mock_db_manager)

        builder.add_numeric_range()

        measurement_sql = str(builder.measurement_query.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True}
        ))
        observation_sql = str(builder.observation_query.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True}
        ))
        condition_sql = str(builder.condition_query.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True}
        ))
        drug_sql = str(builder.drug_query.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True}
        ))

        assert "measurement.person_id" in measurement_sql
        assert "observation.person_id" in observation_sql
        assert "condition_occurrence.person_id" in condition_sql
        assert "drug_exposure.person_id" in drug_sql

    def test_add_numeric_range_with_valid_range(self) -> None: 
        """Test adding a valid numeric range."""
        mock_db_manager = Mock()
        builder = OMOPRuleQueryBuilder(mock_db_manager)

        builder.add_numeric_range()

        builder.add_numeric_range(10.5, 20.5)
        
        measurement_sql = str(builder.measurement_query.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True}
        ))
        observation_sql = str(builder.observation_query.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True}
        ))

        assert "value_as_number BETWEEN 10.5 AND 20.5" in measurement_sql
        assert "value_as_number BETWEEN 10.5 AND 20.5" in observation_sql

    def test_add_numeric_range_with_inverted_range(self) -> None:
        """Test correct error raised when min > max ."""
        mock_db_manager = Mock()
        builder = OMOPRuleQueryBuilder(mock_db_manager)

        with pytest.raises(ValueError): 
            builder.add_numeric_range(20.0, 10.0)
        
    def test_add_secondary_modifiers_empty_list(self) -> None:
        """Test with empty list - no constraints should be added."""
        mock_db_manager = Mock()
        builder = OMOPRuleQueryBuilder(mock_db_manager)

        builder.add_secondary_modifiers([])
        
        # Check all queries remain unchanged
        condition_sql = str(builder.condition_query.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True}
        ))
        measurement_sql = str(builder.measurement_query.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True}
        ))

        assert "condition_type_concept_id" not in condition_sql
        assert "condition_type_concept_id" not in measurement_sql
        assert "condition_occurrence.person_id" in condition_sql
        assert "measurement.person_id" in measurement_sql

    def test_add_secondary_modifiers_single_valid_id(self) -> None:
        """Test with single valid modifier ID."""
        mock_db_manager = Mock()
        builder = OMOPRuleQueryBuilder(mock_db_manager)

        builder.add_secondary_modifiers([32020])
        
        condition_sql = str(builder.condition_query.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True}
        ))
        
        assert "condition_type_concept_id = 32020" in condition_sql
        assert " OR " not in condition_sql
        
        measurement_sql = str(builder.measurement_query.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True}
        ))
        assert "condition_type_concept_id" not in measurement_sql

    @pytest.mark.parametrize("invalid_input", [None, 32020, "32020", {"id": 32020}])
    def test_add_secondary_modifiers_invalid_input_type(self, invalid_input: None | int | str | dict) -> None:
        """Test with invalid input types - should raise appropriate error."""
        mock_db_manager = Mock()
        builder = OMOPRuleQueryBuilder(mock_db_manager)

        with pytest.raises((TypeError, AttributeError)):
            builder.add_secondary_modifiers(invalid_input)

    def test_build_default_queries_no_constraints(self) -> None:
        """Test that build() returns a CompoundSelect (UNION) object."""
        mock_db_manager = Mock()
        builder = OMOPRuleQueryBuilder(mock_db_manager)

        result = builder.build()

        query_str = str(result)

        assert isinstance(result, CompoundSelect)
        assert "measurement.person_id" in query_str
        assert "observation.person_id" in query_str
        assert "condition_occurrence.person_id" in query_str
        assert "drug_exposure.person_id" in query_str
        assert "UNION" in query_str
    
    def test_build_after_adding_concept_constraint(self) -> None:
        """Test build after adding a concept constraint."""
        mock_db_manager = Mock()
        builder = OMOPRuleQueryBuilder(mock_db_manager)

        concept_id = 12345
        builder.add_concept_constraint(concept_id)
        
        result = builder.build()
        compiled = result.compile(compile_kwargs={"literal_binds": True})
        query_str = str(compiled)
        
        assert str(concept_id) in query_str
        assert "condition_concept_id" in query_str
        assert "measurement_concept_id" in query_str
        assert "observation_concept_id" in query_str
        assert "drug_concept_id" in query_str

    def test_build_with_multiple_constraints(self) -> None:
        """Test build with multiple different constraints applied."""
        mock_db_manager = Mock()
        builder = OMOPRuleQueryBuilder(mock_db_manager)

        builder.add_concept_constraint(12345)
        builder.add_numeric_range(5.0, 10.0)
        builder.add_temporal_constraint("6", "")  
        
        result = builder.build()
        compiled = result.compile(compile_kwargs={"literal_binds": True})
        query_str = str(compiled)

        assert "12345" in query_str  
        assert "BETWEEN" in query_str 
        assert "measurement_date" in query_str 
        assert "UNION" in query_str

    def test_build_maintains_union_structure(self) -> None:
        """Test that build always returns a UNION of exactly 4 queries."""
        mock_db_manager = Mock()
        builder = OMOPRuleQueryBuilder(mock_db_manager)

        builder.add_concept_constraint(99999)
        builder.add_numeric_range(1.0, 100.0)
        
        result = builder.build()
        
        # Count UNION occurrences (should be 3 for 4 queries)
        query_str = str(result)
        union_count = query_str.count("UNION")
        
        # 4 queries connected by 3 UNIONs
        assert union_count == 4


class TestPersonQueryConstraintBuilder: 
    @pytest.fixture
    def mock_db_manager(self) -> Mock:
        """Create a mock database manager."""
        mock = Mock()
        mock.engine = Mock()
        mock.engine.dialect = Mock()
        mock.engine.dialect.name = "postgresql"
        return mock

    @pytest.fixture
    def builder(self, mock_db_manager: Mock) -> PersonConstraintBuilder:
        """Create a PersonConstraintBuilder instance."""
        return PersonConstraintBuilder(mock_db_manager) 
    
    def test_build_age_constraints_valid_range(
        self, 
        builder: PersonConstraintBuilder
    ) -> None:
        """Test age constraints produce correct SQL for a given range."""
        rule = Mock()
        rule.min_value = 18.0
        rule.max_value = 65.0

        with patch("hutch_bunny.core.solvers.rule_query_builders.SQLDialectHandler.get_year_difference") as mock_diff: 
            mock_diff.return_value = literal_column("25")
            constraints = builder._build_age_constraints(rule)

        # Expect 2 constraints: >= min_age and <= max_age
        assert len(constraints) == 2

        compiled_sql = [str(c.compile(compile_kwargs={"literal_binds": True})) for c in constraints]

        # We don't need to know the exact CURRENT_TIMESTAMP calculation here,
        # just that it's the right comparison with literal values.
        assert f">= {rule.min_value}" in compiled_sql[0]
        assert f"<= {rule.max_value}" in compiled_sql[1]

    def test_build_age_constraints_none_values(self, builder: PersonConstraintBuilder) -> None:
        """Directly test _build_age_constraints with None values."""
        rule = Mock()
        rule.min_value = None
        rule.max_value = 65.0
        
        result = builder._build_age_constraints(rule)
        
        assert result == []

    def test_build_gender_constraint_with_inclusion(self, builder: PersonConstraintBuilder) -> None:
        """Test gender inclusion produces correct SQL."""
        rule = Mock()
        rule.value = "8507"
        rule.operator = "="
        rule.left_value_time=None
        rule.right_value_time=None

        result = builder._build_gender_constraint(rule, builder._build_age_constraint(rule))
        assert len(result) == 1
        compiled_sql = str(result[0].compile(compile_kwargs={"literal_binds": True}))
        assert compiled_sql == "person.gender_concept_id = 8507"
    
    def test_build_gender_constraint_with_exclusion(self, builder: PersonConstraintBuilder) -> None:
        """Test gender exclusion produces correct SQL."""
        rule = Mock()
        rule.value = "8532"
        rule.operator = "!="
        rule.left_value_time = None
        rule.right_value_time = None

        result = builder._build_gender_constraint(rule, builder._build_age_constraint(rule))
        assert len(result) == 1
        compiled_sql = str(result[0].compile(compile_kwargs={"literal_binds": True}))
        assert compiled_sql == "person.gender_concept_id != 8532"

    def test_build_gender_constraint_directly_invalid_value(self, builder: PersonConstraintBuilder) -> None:
        """Directly test _build_gender_constraint with invalid value."""
        rule = Mock()
        rule.value = "not_a_number"
        rule.operator = "="
        rule.left_value_time = None
        rule.right_value_time = None

        with pytest.raises(ValueError):
            builder._build_gender_constraint(rule, builder._build_age_constraint(rule))

    def test_build_race_constraint(self, builder: PersonConstraintBuilder) -> None:
        """Test race constraint for both inclusion and exclusion."""
        test_cases = [
            ("8516", "=", "person.race_concept_id = 8516"),
            ("8527", "!=", "person.race_concept_id != 8527"),
        ]

        for value, operator, expected_sql in test_cases:
            rule = Mock()
            rule.value = value
            rule.operator = operator
            rule.left_value_time = None
            rule.right_value_time = None
            
            result = builder._build_race_constraint(rule, builder._build_age_constraint(rule))
            assert len(result) == 1
            compiled_sql = str(result[0].compile(compile_kwargs={"literal_binds": True}))
            assert compiled_sql == expected_sql
    
    def test_build_ethnicity_constraint(self, builder: PersonConstraintBuilder) -> None:
        """Test ethnicity constraint for both inclusion and exclusion."""
        test_cases = [
            ("8516", "=", "person.ethnicity_concept_id = 8516"),
            ("8527", "!=", "person.ethnicity_concept_id != 8527"),
        ]

        for value, operator, expected_sql in test_cases:
            rule = Mock()
            rule.value = value
            rule.operator = operator
            rule.left_value_time = None
            rule.right_value_time = None

            result = builder._build_ethnicity_constraint(rule, builder._build_age_constraint(rule))
            assert len(result) == 1
            compiled_sql = str(result[0].compile(compile_kwargs={"literal_binds": True}))
            assert compiled_sql == expected_sql

    @pytest.mark.parametrize(
        "rule_setup, expected_sql_parts",
            [
                # Age rule (min/max)
                (
                    {"varname": "AGE", "min_value": 18.0, "max_value": 65.0},
                    ["person.year_of_birth", ">=", "18.0"]
                ),
                # Gender inclusion
                (
                    {"type": "gender", "value": "8507", "operator": "="},
                    ["person.gender_concept_id = 8507"]
                ),
                # Race exclusion
                (
                    {"type": "race", "value": "8527", "operator": "!="},
                    ["person.race_concept_id != 8527"]
                ),
                # Ethnicity inclusion
                (
                    {"type": "ethnicity", "value": "8100", "operator": "="},
                    ["person.ethnicity_concept_id = 8100"]
                ),
            ]
    )
    def test_build_constraints_single_rule(
        self, 
        rule_setup: dict[str, str | float], 
        expected_sql_parts: list[str], 
        builder: PersonConstraintBuilder
    ) -> None:
        """Test build_constraints returns correct SQL for a single rule type."""
        rule = Mock(**rule_setup)

        concepts = {
            "8507": "Gender", 
            "8527": "Race", 
            "8100": "Ethnicity"
        }

        constraints = builder.build_constraints(rule, concepts)

        assert isinstance(constraints, list)
        assert len(constraints) >= 1

        compiled_sql = str(constraints[0].compile(compile_kwargs={"literal_binds": True}))

        # Check expected SQL fragments or exact match
        for part in expected_sql_parts:
            assert part in compiled_sql
