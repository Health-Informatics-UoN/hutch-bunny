from dataclasses import dataclass
from typing import Optional
import json


@dataclass
class AvailabilityTestCase:
    """Test case for availability queries.

    This class represents a test case for availability queries, with support for
    rounding and low number suppression modifiers.
    The test case can be converted to the JSON format required by the CLI using get_modifiers_json().

    Attributes:
        json_file_path: Path to the JSON query file
        expected_count: Expected count of results
        rounding: Optional rounding parameter (nearest value)
        low_number_suppression: Optional threshold for low number suppression
    """

    json_file_path: str
    expected_count: int
    rounding: Optional[int] = None
    low_number_suppression: Optional[int] = None

    def get_modifiers_json(self) -> str:
        """Convert the modifiers to a JSON string format."""
        modifiers_list = []
        if self.rounding is not None:
            modifiers_list.append({"id": "Rounding", "nearest": self.rounding})
        if self.low_number_suppression is not None:
            modifiers_list.append(
                {
                    "id": "Low Number Suppression",
                    "threshold": self.low_number_suppression,
                }
            )
        return json.dumps(modifiers_list)


test_cases = [
    # Basic availability test - assert default rounding.
    # Query for 8507 MALE
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/availability.json",
        expected_count=570,
    ),
    # Basic availability test - assert rounding to 0.
    # Query for 8507 MALE
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/availability.json",
        rounding=0,
        expected_count=572,
    ),
    # Basic availability test - assert overriden rounding and low number suppression.
    # Query for 8507 MALE
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/availability.json",
        rounding=0,
        low_number_suppression=0,
        expected_count=572,
    ),
    # Basic availability test - assert overriden rounding.
    # Query for 8507 MALE
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/availability.json",
        low_number_suppression=30,
        expected_count=570,
    ),
    # Basic availability test - assert low number suppression on threshold.
    # Query for 8507 MALE
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/availability.json",
        low_number_suppression=570,
        expected_count=570,
    ),
    # Basic availability test - assert rounding and low number suppression.
    # Query for 8507 MALE
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/availability.json",
        rounding=10,
        low_number_suppression=20,
        expected_count=570,
    ),
    # Basic availability test - assert rounding to 100.
    # Query for 8507 MALE
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/availability.json",
        rounding=100,
        expected_count=600,
    ),
    # Basic availability test - assert rounding to 10.
    # Query for 8507 MALE
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/availability.json",
        rounding=10,
        expected_count=570,
    ),
    # Basic gender test - assert gender OR filtering with default rounding.
    # Query for 8507 MALE OR 8532 FEMALE
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/basic_gender_or.json",
        expected_count=1130,
    ),
    # Basic gender test - assert gender OR filtering with rounding to 0.
    # Query for 8507 MALE OR 8532 FEMALE
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/basic_gender_or.json",
        rounding=0,
        expected_count=1130,
    ),
    # Multiple in group test - assert multiple in group AND filtering with default rounding.
    # Query for 8532 FEMALE AND 260139 Acute Bronchitis
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/multiple_in_group_and.json",
        expected_count=220,
    ),
    # Mutiple in group test - assert multiple in group OR filtering with default rounding.
    # Query for 8532 FEMALE OR 260139 Acute Bronchitis
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/multiple_in_group_or.json",
        expected_count=780,
    ),
    # Mutiple in group test - assert multiple in group OR filtering with rounding to 0.
    # Query for 8532 FEMALE OR 260139 Acute Bronchitis
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/multiple_in_group_or.json",
        rounding=0,
        expected_count=782,
    ),
    # Mutiple in group test - assert multiple in group AND filtering with age 1.
    # 8532 Female AND 432867 Hyperlipidemia
    # Age > 50 years when it occurred
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/multiple_in_group_and_with_age1.json",
        expected_count=30,
    ),
    # Mutiple in group test - assert multiple in group AND filtering with age 1 and rounding to 0.
    # 8532 Female AND 432867 Hyperlipidemia
    # Age > 50 years when it occurred
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/multiple_in_group_and_with_age1.json",
        rounding=0,
        expected_count=26,
    ),
    # Mutiple in group test - assert multiple in group OR filtering with age 2.
    # 8532 Female AND 432867 Hyperlipidemia
    # Age < 50 years when it occurred
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/multiple_in_group_and_with_age2.json",
        expected_count=20,
    ),
    # Mutiple in group test - assert multiple in group OR filtering with age 2 and rounding to 0.
    # 8532 Female AND 432867 Hyperlipidemia
    # Age < 50 years when it occurred
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/multiple_in_group_and_with_age2.json",
        rounding=0,
        expected_count=21,
    ),
    # Basic measurement test - assert measurement with default rounding.
    # 46236952 Glomerular filtration
    # > 1.0, < 3.0
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/measurement.json",
        rounding=0,
        low_number_suppression=10,
        expected_count=329,
    ),
    # Multiple in group, exclusion criteria, and time test.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/multiple_in_group_exclusion_time.json",
        rounding=0,
        low_number_suppression=10,
        expected_count=442,
    ),
    # Basic ethnicity test - assert ethnicity OR filtering with override rounding.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/basic_ethnicity_or.json",
        rounding=0,
        low_number_suppression=0,
        expected_count=112,
    ),
    # Basic race test - assert race OR filtering with override rounding.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/basic_race_or.json",
        rounding=0,
        low_number_suppression=0,
        expected_count=82,
    ),
    # Secondary modifiers test - assert secondary modifiers with override rounding.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/secondary_modifiers.json",
        rounding=0,
        low_number_suppression=0,
        expected_count=333,
    ),
]

# TODO: Add test cases for age - until we have improved code patterns for datetime.
todo_test_cases = [
    # Age - assert age filtering with default rounding.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/age.json",
        rounding=10,
        low_number_suppression=10,
        expected_count=10,
    ),
    # Age - assert age filtering with override rounding.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/age.json",
        rounding=0,
        low_number_suppression=0,
        expected_count=11,
    ),
]
