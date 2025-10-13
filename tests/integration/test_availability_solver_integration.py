from unittest.mock import Mock, patch
from datetime import datetime
import pytest 
from sqlalchemy import select 

from hutch_bunny.core.rquest_models.rule import Rule
from hutch_bunny.core.rquest_models.group import Group
from hutch_bunny.core.db.entities import Person 
from hutch_bunny.core.rquest_models.availability import AvailabilityQuery
from hutch_bunny.core.db.sync import SyncDBClient
from hutch_bunny.core.solvers.availability_solver import AvailabilitySolver


@pytest.mark.integration
class TestBuildRuleQuery: 
    def test_condition_concept_query(self, db_client: SyncDBClient) -> None:
        """Test query generation for a condition concept."""
        rule = Rule(
            varname="OMOP",
            varcat="Condition", 
            type_="TEXT",
            operator="=",
            value="260139" 
        )

        mock_query = Mock(spec=AvailabilityQuery)
        availability_solver = AvailabilitySolver(db_client, mock_query)
        
        query = availability_solver._build_rule_query(rule)
        
        # Execute and verify structure
        with db_client.engine.connect() as conn:
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
    
    def test_measurement_with_range(self, db_client: SyncDBClient) -> None:
        """Test measurement query with numeric range."""
        rule = Rule(
            varname="OMOP=46236952",  # Glomerular filtration
            varcat="Measurement",
            type_="NUM",
            operator="=",
            value="1.0|3.0"  
        )

        mock_query = Mock(spec=AvailabilityQuery)
        availability_solver = AvailabilitySolver(db_client, mock_query)
        
        query = availability_solver._build_rule_query(rule)
        
        with db_client.engine.connect() as conn:
            result = conn.execute(query)
            rows = result.fetchall()
            
            # Verify the query filters by value_as_number
            sql_str = str(query.compile(compile_kwargs={"literal_binds": True}))
            assert "value_as_number BETWEEN" in sql_str
            assert "46236952" in sql_str
            
            # Check actual results
            assert len(rows) > 0
    
    def test_age_at_diagnosis_greater_than(self, db_client: SyncDBClient) -> None:
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
        availability_solver = AvailabilitySolver(db_client, mock_query)

        query = availability_solver._build_rule_query(rule)
        
        with db_client.engine.connect() as conn:
            result = conn.execute(query)
            person_ids = {row[0] for row in result}
            
            # Verify age calculation in SQL
            sql_str = str(query.compile(compile_kwargs={"literal_binds": True}))
            if db_client.engine.dialect.name == "postgresql":
                assert "date_part" in sql_str
            elif db_client.engine.dialect.name == "mssql":
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
    
    def test_time_relative_constraint(self, db_client: SyncDBClient) -> None:
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
        availability_solver = AvailabilitySolver(db_client, mock_query)
        
        with patch("hutch_bunny.core.solvers.rule_query_builders.datetime") as mock_datetime: 
            fixed_now = datetime(2025, 8, 7, 12, 0, 0)
            mock_datetime.now.return_value = fixed_now
            query = availability_solver._build_rule_query(rule)
        
        with db_client.engine.connect() as conn:
            result = conn.execute(query)
            person_ids = {row[0] for row in result}
            assert len(person_ids) > 0
            
            sql_str = str(query.compile(compile_kwargs={"literal_binds": True}))

            assert "condition_start_date" in sql_str
            assert "2025-07" in sql_str  # One month back from frozen time
        
    def test_secondary_modifier_single(self, db_client: SyncDBClient) -> None:
        """Test secondary modifier for condition provenance."""
        rule = Rule(
            varname="OMOP",
            varcat="Condition",
            type_="TEXT",
            operator="=",
            value="260139",
            secondary_modifier=[32020]  # EHR encounter diagnosis
        )

        mock_query = Mock(spec=AvailabilityQuery)
        availability_solver = AvailabilitySolver(db_client, mock_query)
        query = availability_solver._build_rule_query(rule)
        
        with db_client.engine.connect() as conn:
            result = conn.execute(query)
            person_ids = {row[0] for row in result}
            assert len(person_ids) > 1 

            sql_str = str(query.compile(compile_kwargs={"literal_binds": True}))
            assert "condition_type_concept_id" in sql_str
            assert "32020" in sql_str
    
    def test_secondary_modifier_multiple(self, db_client: SyncDBClient) -> None:
        """Test multiple secondary modifiers with OR logic."""
        rule = Rule(
            varname="OMOP",
            varcat="Condition",
            type_="TEXT",
            operator="=",
            value="260139",
            secondary_modifier=[32020, 32021, 32022]
        )
        
        mock_query = Mock(spec=AvailabilityQuery)
        availability_solver = AvailabilitySolver(db_client, mock_query)
        query = availability_solver._build_rule_query(rule)
        
        sql_str = str(query.compile(compile_kwargs={"literal_binds": True}))
        assert sql_str.count("condition_type_concept_id") >= 3
        assert "OR" in sql_str

        with db_client.engine.connect() as conn:
            result = conn.execute(query)
            person_ids = {row[0] for row in result}
            assert len(person_ids) > 1 
    
    def test_measurement_with_range_and_time(self, db_client: SyncDBClient) -> None:
        """Test measurement with both numeric range and temporal constraint."""
        rule = Rule(
            varname="OMOP=4078285",
            varcat="Measurement",
            type_="NUM",
            operator="=",
            value="1.0|6.0",
            time="|6:TIME:M"  # Less than 6 months ago
        )
        
        mock_query = Mock(spec=AvailabilityQuery)
        availability_solver = AvailabilitySolver(db_client, mock_query)
        
        with patch("hutch_bunny.core.solvers.rule_query_builders.datetime") as mock_datetime: 
            fixed_now = datetime(2015, 10, 2, 12, 0, 0)
            mock_datetime.now.return_value = fixed_now
            query = availability_solver._build_rule_query(rule)
            with db_client.engine.connect() as conn:
                result = conn.execute(query)
                person_ids = {row[0] for row in result}

                assert len(person_ids) > 1 

                sql_str = str(query.compile(compile_kwargs={"literal_binds": True}))
                assert "value_as_number BETWEEN" in sql_str
                assert "measurement_date" in sql_str

    def test_condition_with_age_and_modifiers(self, db_client: SyncDBClient) -> None:
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
        availability_solver = AvailabilitySolver(db_client, mock_query)

        query = availability_solver._build_rule_query(rule)
        
        sql_str = str(query.compile(compile_kwargs={"literal_binds": True}))
        assert "condition_type_concept_id" in sql_str
        assert "year_of_birth" in sql_str

    def test_age_rule_on_person_table(self, db_client: SyncDBClient) -> None: 
        """Test an AGE rule that filters the Person table by current age."""
        rule = Rule(
            varname="AGE", 
            varcat="Person", 
            type_="NUM", 
            operator="=", 
            value="20|50"
        ) 

        mock_query = Mock(spec=AvailabilityQuery)
        availability_solver = AvailabilitySolver(db_client, mock_query)
        
        group = Group(
            rules=[rule],
            rules_operator="AND"
        )
        concepts = {}
        query = availability_solver._build_group_query(group, concepts)
        sql_str = str(query.compile(compile_kwargs={"literal_binds": True}))

        if db_client.engine.dialect.name == "postgresql":
            assert "date_part" in sql_str.lower() or "extract" in sql_str.lower()
            assert "year_of_birth" in sql_str
        elif db_client.engine.dialect.name == "mssql":
            assert "DATEPART" in sql_str
            assert "year_of_birth" in sql_str
        
        assert ">= 20" in sql_str or ">=20" in sql_str
        assert "<= 50" in sql_str or "<=50" in sql_str


@pytest.mark.integration
class TestBuildGroupQuery: 
    @pytest.fixture
    def availability_solver(self, db_client: SyncDBClient) -> AvailabilitySolver:
        """Create an AvailabilitySolver with a real database connection."""
        mock_query = Mock(spec=AvailabilityQuery)
        return AvailabilitySolver(db_client, mock_query)

    @pytest.fixture
    def concepts_dict(self) -> dict[str, str]:
        """Common concepts mapping for tests."""
        return {
            "8507": "Gender",      # Male
            "8532": "Gender",      # Female
            "260139": "Condition", # Acute Bronchitis
            "432867": "Condition", # Hyperlipidemia
            "19115351": "Drug",    # Diazepam
            "8516": "Race",        # Black or African American
            "38003563": "Ethnicity" # Hispanic or Latino
        }

    def test_single_inclusion_person_rule(
        self, 
        db_client: SyncDBClient, 
        availability_solver: AvailabilitySolver, 
        concepts_dict: dict[str, str]
    ) -> None:
        """Test group with single Person inclusion rule."""
        group = Group(
            rules=[
                Rule(
                    varname="OMOP",
                    varcat="Person",
                    type_="TEXT",
                    operator="=",
                    value="8507"  # Male
                )
            ],
            rules_operator="AND"
        )
        
        query = availability_solver._build_group_query(group, concepts_dict)
        
        with db_client.engine.connect() as conn:
            result = conn.execute(query)
            person_ids = {row[0] for row in result}
            
            assert len(person_ids) > 0
            
            sql_str = str(query.compile(compile_kwargs={"literal_binds": True}))
            assert "person.gender_concept_id = 8507" in sql_str
    
    def test_single_exclusion_person_rule(
        self, 
        db_client: SyncDBClient,
        availability_solver: AvailabilitySolver, 
        concepts_dict: dict[str, str]
    ) -> None:
        """Test group with single Person exclusion rule."""
        group = Group(
            rules=[
                Rule(
                    varname="OMOP",
                    varcat="Person",
                    type_="TEXT",
                    operator="!=",  # Exclusion
                    value="8507"  # Not Male
                )
            ],
            rules_operator="AND"
        )
        
        query = availability_solver._build_group_query(group, concepts_dict)
        
        with db_client.engine.connect() as conn:
            result = conn.execute(query)
            person_ids = {row[0] for row in result}
            
            assert len(person_ids) > 0
            
            sql_str = str(query.compile(compile_kwargs={"literal_binds": True}))
            assert "person.gender_concept_id != 8507" in sql_str
    
    def test_multiple_exclusions_person_rule_with_and(
        self,
        db_client: SyncDBClient, 
        availability_solver: AvailabilitySolver,
        concepts_dict: dict[str, str]
    ) -> None:
        """Test multiple Person exclusions with AND logic."""
        group = Group(
            rules=[
                Rule(
                    varname="OMOP",
                    varcat="Person",
                    type_="TEXT",
                    operator="!=",  # Not male
                    value="8507"
                ),
                Rule(
                    varname="OMOP",
                    varcat="Person",
                    type_="TEXT",
                    operator="!=",  # Not Black race
                    value="8516"
                )
            ],
            rules_operator="AND"
        )
        
        query = availability_solver._build_group_query(group, concepts_dict)
        
        with db_client.engine.connect() as conn:
            result = conn.execute(query)
            person_ids = {row[0] for row in result}
            
            # Should get females who are not Black
            # This is the intersection of "not male" AND "not Black"
            
            # Get females
            female_query = select(Person.person_id).where(
                Person.gender_concept_id == 8532
            )
            female_result = conn.execute(female_query)
            female_ids = {row[0] for row in female_result}
            
            # Get Black individuals
            black_query = select(Person.person_id).where(
                Person.race_concept_id == 8516
            )
            black_result = conn.execute(black_query)
            black_ids = {row[0] for row in black_result}
            
            assert len(person_ids) < len(female_ids)  # Less than all females
            assert len(person_ids) > 400  # But still substantial
            
            # No males in result
            male_query = select(Person.person_id).where(
                Person.gender_concept_id == 8507
            )
            male_result = conn.execute(male_query)
            male_ids = {row[0] for row in male_result}
            assert len(person_ids.intersection(male_ids)) == 0
    
    def test_single_inclusion_omop_rule(
        self, 
        db_client: SyncDBClient, 
        availability_solver: AvailabilitySolver, 
        concepts_dict: dict[str, str]
    ) -> None:
        """Test group with single OMOP inclusion rule."""
        group = Group(
            rules=[
                Rule(
                    varname="OMOP",
                    varcat="Condition",
                    type_="TEXT",
                    operator="=",
                    value="260139"  # Acute Bronchitis
                )
            ],
            rules_operator="AND"
        )
        
        query = availability_solver._build_group_query(group, concepts_dict)
        
        with db_client.engine.connect() as conn:
            result = conn.execute(query)
            person_ids = {row[0] for row in result}
            
            assert len(person_ids) > 400  # ~442 expected
            assert len(person_ids) < 500
            
            sql_str = str(query.compile(compile_kwargs={"literal_binds": True}))
            assert "UNION" in sql_str  # OMOP rules create UNIONs

    def test_single_exclusion_omop_rule(
        self, 
        db_client: SyncDBClient, 
        availability_solver: AvailabilitySolver, 
        concepts_dict: dict[str, str]
    ) -> None:
        """Test group with single OMOP exclusion rule."""
        group = Group(
            rules=[
                Rule(
                    varname="OMOP",
                    varcat="Condition",
                    type_="TEXT",
                    operator="!=",  
                    value="35626061"  # Some rare condition
                )
            ],
            rules_operator="AND"
        )
        
        query = availability_solver._build_group_query(group, concepts_dict)
        
        with db_client.engine.connect() as conn:
            result = conn.execute(query)
            person_ids = {row[0] for row in result}
            
            # Should exclude people with this condition
            # Most people won't have it, so should be close to total
            assert len(person_ids) > 1000

    def test_two_person_rules_and(
        self, 
        db_client: SyncDBClient, 
        availability_solver: AvailabilitySolver, 
        concepts_dict: dict[str, str]
    ) -> None:
        """Test group with two Person rules combined with AND."""
        group = Group(
            rules=[
                Rule(
                    varname="OMOP",
                    varcat="Person",
                    type_="TEXT",
                    operator="=",
                    value="8532"  # Female
                ),
                Rule(
                    varname="OMOP",
                    varcat="Person",
                    type_="TEXT",
                    operator="=",
                    value="8516"  # Black or African American
                )
            ],
            rules_operator="AND"
        )
        
        query = availability_solver._build_group_query(group, concepts_dict)
        
        with db_client.engine.connect() as conn:
            result = conn.execute(query)
            person_ids = {row[0] for row in result}
            
            # Should get only Black females - a subset
            assert len(person_ids) < 100  # Small intersection
            
            sql_str = str(query.compile(compile_kwargs={"literal_binds": True}))
            assert "gender_concept_id = 8532" in sql_str
            assert "race_concept_id = 8516" in sql_str

    def test_multiple_omop_rules_and(
        self, 
        db_client: SyncDBClient, 
        availability_solver: AvailabilitySolver, 
        concepts_dict: dict[str, str]
    ) -> None:
        """Test group with multiple OMOP rules combined with AND."""
        group = Group(
            rules=[
                Rule(
                    varname="OMOP",
                    varcat="Condition",
                    type_="TEXT",
                    operator="=",
                    value="260139"  # Acute Bronchitis
                ),
                Rule(
                    varname="OMOP",
                    varcat="Condition",
                    type_="TEXT",
                    operator="=",
                    value="432867"  # Hyperlipidemia
                )
            ],
            rules_operator="AND"
        )
        
        query = availability_solver._build_group_query(group, concepts_dict)
        
        with db_client.engine.connect() as conn:
            result = conn.execute(query)
            person_ids = {row[0] for row in result}
            
            # People with both conditions - should be small
            assert len(person_ids) > 0 
            assert len(person_ids) < 100
            
            sql_str = str(query.compile(compile_kwargs={"literal_binds": True}))
            assert "INTERSECT" in sql_str

    def test_two_person_rules_or(
        self, 
        db_client: SyncDBClient,
        availability_solver: AvailabilitySolver, 
        concepts_dict: dict[str, str]
    ) -> None:
        """Test group with two Person rules combined with OR."""
        group = Group(
            rules=[
                Rule(
                    varname="OMOP",
                    varcat="Person",
                    type_="TEXT",
                    operator="=",
                    value="8507"  # Male
                ),
                Rule(
                    varname="OMOP",
                    varcat="Person",
                    type_="TEXT",
                    operator="=",
                    value="8532"  # Female
                )
            ],
            rules_operator="OR"
        )
        
        query = availability_solver._build_group_query(group, concepts_dict)

        sql_str = str(query.compile(compile_kwargs={"literal_binds": True}))
        assert "gender_concept_id = 8507 OR" in sql_str or "(person.gender_concept_id = 8507) OR (person.gender_concept_id = 8532)" in sql_str
        assert "gender_concept_id = 8507 AND person.gender_concept_id = 8532" not in sql_str
        
        with db_client.engine.connect() as conn:
            result = conn.execute(query)
            person_ids = {row[0] for row in result}
            # Should get all people (male OR female)
            assert len(person_ids) > 1100  # ~1130 total

            # Compare with individual queries
            male_query = select(Person.person_id).where(Person.gender_concept_id == 8507)
            female_query = select(Person.person_id).where(Person.gender_concept_id == 8532)
            
            male_result = conn.execute(male_query)
            female_result = conn.execute(female_query)
            
            male_ids = {row[0] for row in male_result}
            female_ids = {row[0] for row in female_result}
            
            # OR should give us the union of both sets
            expected = male_ids.union(female_ids)
            assert person_ids == expected

    def test_inclusion_and_exclusion_and_logic(
        self, 
        db_client: SyncDBClient, 
        availability_solver: AvailabilitySolver, 
        concepts_dict: dict[str, str]
    ) -> None:
        """Test group with inclusion and exclusion rules with AND."""
        group = Group(
            rules=[
                Rule(
                    varname="OMOP",
                    varcat="Person",
                    type_="TEXT",
                    operator="=",  # Inclusion
                    value="8532"  # Female
                ),
                Rule(
                    varname="OMOP",
                    varcat="Condition",
                    type_="TEXT",
                    operator="!=",  # Exclusion
                    value="432867"  # NOT Hyperlipidemia
                )
            ],
            rules_operator="AND"
        )
        
        query = availability_solver._build_group_query(group, concepts_dict)
        
        with db_client.engine.connect() as conn:
            result = conn.execute(query)
            person_ids = {row[0] for row in result}
            
            # Females without hyperlipidemia
            # Should be less than total females
            result_all_females = conn.execute(
                availability_solver._build_group_query(
                    Group(
                        rules=[Rule(varname="OMOP", varcat="Person", 
                                  type_="TEXT", operator="=", value="8532")],
                        rules_operator="AND"
                    ),
                    concepts_dict
                )
            )
            all_females = {row[0] for row in result_all_females}
            
            assert len(person_ids) < len(all_females)
            assert len(person_ids) > 400  # Most females won't have hyperlipidemia
    
    def test_complex_mixed_rules(
        self, 
        db_client: SyncDBClient, 
        availability_solver: AvailabilitySolver, 
        concepts_dict: dict[str, str]
    ) -> None:
        """Test complex group with multiple inclusion and exclusion rules."""
        group = Group(
            rules=[
                Rule(
                    varname="OMOP",
                    varcat="Person",
                    type_="TEXT",
                    operator="=",
                    value="8532"  # Female
                ),
                Rule(
                    varname="OMOP",
                    varcat="Condition",
                    type_="TEXT",
                    operator="=",
                    value="260139"  # Acute Bronchitis
                ),
                Rule(
                    varname="OMOP",
                    varcat="Drug",
                    type_="TEXT",
                    operator="!=",
                    value="19115351"  # NOT Diazepam
                )
            ],
            rules_operator="AND"
        )

        query = availability_solver._build_group_query(group, concepts_dict)
        
        with db_client.engine.connect() as conn:
            result = conn.execute(query)
            person_ids = {row[0] for row in result}
            
            # Females with bronchitis but not on Diazepam
            assert len(person_ids) > 150
            assert len(person_ids) < 250
    
    def test_group_with_age_constraints(
        self, 
        db_client: SyncDBClient, 
        availability_solver: AvailabilitySolver, 
        concepts_dict: dict[str, str]
    ) -> None:
        """Test group with age-constrained rules."""
        group = Group(
            rules=[
                Rule(
                    varname="OMOP",
                    varcat="Person",
                    type_="TEXT",
                    operator="=",
                    value="8532"  # Female
                ),
                Rule(
                    varname="OMOP",
                    varcat="Condition",
                    type_="TEXT",
                    operator="=",
                    value="432867",  # Hyperlipidemia
                    time="50|:AGE:Y"  # After age 50
                )
            ],
            rules_operator="AND"
        )
        
        query = availability_solver._build_group_query(group, concepts_dict)
        
        with db_client.engine.connect() as conn:
            result = conn.execute(query)
            person_ids = {row[0] for row in result}
            
            # Females with late-onset hyperlipidemia
            assert len(person_ids) < 50  # Should be relatively rare
            
            sql_str = str(query.compile(compile_kwargs={"literal_binds": True}))
            assert "year_of_birth" in sql_str or "date_part" in sql_str.lower()

    def test_group_with_time_constraints(
        self, 
        db_client: SyncDBClient, 
        availability_solver: AvailabilitySolver, 
        concepts_dict: dict[str, str]
    ) -> None:
        """Test group with time-relative constraints."""
        group = Group(
            rules=[
                Rule(
                    varname="OMOP",
                    varcat="Condition",
                    type_="TEXT",
                    operator="=",
                    value="260139",  # Acute Bronchitis
                    time="1|:TIME:M"  # Greater than a month (ago)
                ),
                Rule(
                    varname="OMOP",
                    varcat="Condition",
                    type_="TEXT",
                    operator="=",
                    value="260139",  # Same condition
                    time="|1:TIME:M"  # Less than a month ago 
                )
            ],
            rules_operator="OR"  # Either recent or old
        )
        
        with patch("hutch_bunny.core.solvers.rule_query_builders.datetime") as mock_datetime: 
            fixed_now = datetime(2015, 10, 2, 12, 0, 0)
            mock_datetime.now.return_value = fixed_now
            query = availability_solver._build_group_query(group, concepts_dict)
        
        with db_client.engine.connect() as conn:
            result = conn.execute(query)
            person_ids = {row[0] for row in result}
            
            # Should get people with bronchitis at any time
            assert len(person_ids) > 400
    
    def test_group_with_measurement_ranges(
        self, 
        db_client: SyncDBClient, 
        availability_solver: AvailabilitySolver, 
        concepts_dict: dict[str, str]
    ) -> None:
        """Test group with measurement value ranges."""
        group = Group(
            rules=[
                Rule(
                    varname="OMOP=46236952",  # Glomerular filtration
                    varcat="Measurement",
                    type_="NUM",
                    operator="=",
                    value="1.0|3.0"  # Range
                ),
                Rule(
                    varname="OMOP",
                    varcat="Person",
                    type_="TEXT",
                    operator="=",
                    value="8507"  # Male
                )
            ],
            rules_operator="AND"
        )
        
        query = availability_solver._build_group_query(group, concepts_dict)
        
        with db_client.engine.connect() as conn:
            result = conn.execute(query)
            person_ids = {row[0] for row in result}
            
            # Males with specific GFR range
            assert len(person_ids) > 0
            
            sql_str = str(query.compile(compile_kwargs={"literal_binds": True}))
            assert "value_as_number BETWEEN" in sql_str
    
    def test_empty_group_results(
        self, 
        db_client: SyncDBClient, 
        availability_solver: AvailabilitySolver, 
        concepts_dict: dict[str, str]
    ) -> None:
        """Test group that produces no results."""
        group = Group(
            rules=[
                Rule(
                    varname="OMOP",
                    varcat="Person",
                    type_="TEXT",
                    operator="=",
                    value="8507"  # Male
                ),
                Rule(
                    varname="OMOP",
                    varcat="Person",
                    type_="TEXT",
                    operator="=",
                    value="8532"  # Female
                )
            ],
            rules_operator="AND"  # Impossible: Male AND Female
        )
        
        query = availability_solver._build_group_query(group, concepts_dict)
        
        with db_client.engine.connect() as conn:
            result = conn.execute(query)
            person_ids = {row[0] for row in result}
            
            assert len(person_ids) == 0
    