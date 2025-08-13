from unittest.mock import Mock, patch
import pytest 

from hutch_bunny.core.rquest_models.rule import Rule
from hutch_bunny.core.rquest_models.availability import AvailabilityQuery
from hutch_bunny.core.db_manager import SyncDBManager
from hutch_bunny.core.solvers.availability_solver import AvailabilitySolver


class TestBuildRuleQuery: 
    def test_condition_concept_query(self, db_manager: SyncDBManager) -> None:
        """Test query generation for a condition concept."""
        rule = Rule(
            varname="OMOP",
            varcat="Condition", 
            type_="TEXT",
            operator="=",
            value="260139" 
        )

        mock_query = Mock(spec=AvailabilityQuery)
        availability_solver = AvailabilitySolver(db_manager, mock_query)
        
        query = availability_solver._build_rule_query(rule)
        
        # Execute and verify structure
        with db_manager.engine.connect() as conn:
            result = conn.execute(query)
            rows = result.fetchall()
            
            # Verify it returns person_ids
            assert all(isinstance(row[0], int) for row in rows)
            
            # Verify the SQL contains expected tables
            sql_str = str(query.compile(compile_kwargs={"literal_binds": True}))
            assert "condition_occurrence" in sql_str
            assert "measurement" in sql_str
            assert "observation" in sql_str
            assert "drug_exposure" in sql_str
            assert "UNION" in sql_str
    
    def test_measurement_with_range(self, db_manager: SyncDBManager) -> None:
        """Test measurement query with numeric range."""
        rule = Rule(
            varname="OMOP=46236952",  # Glomerular filtration
            varcat="Measurement",
            type_="NUM",
            operator="=",
            value="1.0|3.0"  
        )

        mock_query = Mock(spec=AvailabilityQuery)
        availability_solver = AvailabilitySolver(db_manager, mock_query)
        
        query = availability_solver._build_rule_query(rule)
        
        with db_manager.engine.connect() as conn:
            result = conn.execute(query)
            rows = result.fetchall()
            
            # Verify the query filters by value_as_number
            sql_str = str(query.compile(compile_kwargs={"literal_binds": True}))
            assert "value_as_number BETWEEN" in sql_str
            assert "46236952" in sql_str
            
            # Check actual results
            assert len(rows) > 0
    
    def test_age_at_diagnosis_greater_than(self, db_manager: SyncDBManager) -> None:
        """Test age constraint for diagnosis after certain age."""
        rule = Rule(
            varname="OMOP",
            varcat="Condition",
            type_="TEXT",
            operator="=",
            value="432867",  # Hyperlipidemia
            time="50|:AGE:Y"  # After age 50
        )
        
        mock_query = Mock(spec=AvailabilityQuery)
        availability_solver = AvailabilitySolver(db_manager, mock_query)

        query = availability_solver._build_rule_query(rule)
        
        with db_manager.engine.connect() as conn:
            result = conn.execute(query)
            person_ids = {row[0] for row in result}
            
            # Verify age calculation in SQL
            sql_str = str(query.compile(compile_kwargs={"literal_binds": True}))
            if db_manager.engine.dialect.name == "postgresql":
                assert "date_part" in sql_str
            elif db_manager.engine.dialect.name == "mssql":
                assert "DATEPART" in sql_str
            
            # Should have fewer results than without age constraint
            rule_no_age = Rule(
                varname="OMOP",
                varcat="Condition", 
                type_="TEXT",
                operator="=",
                value="432867"
            )
            query_no_age = availability_solver._build_rule_query(rule_no_age)
            result_no_age = conn.execute(query_no_age)
            person_ids_no_age = {row[0] for row in result_no_age}
            
            assert len(person_ids) < len(person_ids_no_age)
    
    def test_time_relative_constraint(self, db_manager: SyncDBManager) -> None:
        """Test time-relative constraints (within last X months)."""
        rule = Rule(
            varname="OMOP",
            varcat="Condition",
            type_="TEXT",
            operator="=",
            value="260139",
            time="1|:TIME:M"  # Within last month
        )

        mock_query = Mock(spec=AvailabilityQuery)
        availability_solver = AvailabilitySolver(db_manager, mock_query)
        
        with patch("hutch_bunny.core.solvers.availability_solver.SQLDialectHandler.get_year_difference") as mock_date: 
            query = availability_solver._build_rule_query(rule)
        
        with db_manager.engine.connect() as conn:
            result = conn.execute(query)
            person_ids = {row[0] for row in result}
            assert len(person_ids) > 0
            
            sql_str = str(query.compile(compile_kwargs={"literal_binds": True}))

            assert "condition_start_date" in sql_str
            assert "2025-07" in sql_str  # One month back from frozen time
    
    def test_measurement_with_range_and_time(self, db_manager: SyncDBManager) -> None:
        """Test measurement with both numeric range and temporal constraint."""
        rule = Rule(
            varname="OMOP=46236952",
            varcat="Measurement",
            type_="NUM",
            operator="=",
            value="1.0|3.0",
            time="|6:TIME:M"  # More than 6 months ago
        )
        
        mock_query = Mock(spec=AvailabilityQuery)
        availability_solver = AvailabilitySolver(db_manager, mock_query)

        query = availability_solver._build_rule_query(rule)
        
        with db_manager.engine.connect() as conn:
            result = conn.execute(query)
            person_ids = {row[0] for row in result}
            assert len(person_ids) > 0
            
            sql_str = str(query.compile(compile_kwargs={"literal_binds": True}))
            assert "value_as_number BETWEEN" in sql_str
            assert "measurement_date" in sql_str

    def test_condition_with_age_and_modifiers(self, db_manager: SyncDBManager) -> None:
        """Test condition with age constraint and secondary modifiers."""
        rule = Rule(
            varname="OMOP",
            varcat="Condition",
            type_="TEXT",
            operator="=",
            value="432867",
            time="|50:AGE:Y",  # Before age 50
            secondary_modifier=[32020]
        )
        
        mock_query = Mock(spec=AvailabilityQuery)
        availability_solver = AvailabilitySolver(db_manager, mock_query)

        query = availability_solver._build_rule_query(rule)
        
        sql_str = str(query.compile(compile_kwargs={"literal_binds": True}))
        assert "condition_type_concept_id" in sql_str
        assert "year_of_birth" in sql_str


@pytest.mark.integration
class TestBuildGroupQuery: 
    pass