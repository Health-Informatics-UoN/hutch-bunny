from dataclasses import dataclass
from typing import Optional
import json


@dataclass
class AvailabilityTestCase:
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
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/availability.json",
        expected_count=40,
    ),
    # Basic availability test - assert rounding to 0.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/availability.json",
        rounding=0,
        expected_count=44,
    ),
    # Basic availability test - assert overriden rounding and low number suppression.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/availability.json",
        rounding=0,
        low_number_suppression=0,
        expected_count=44,
    ),
    # Basic availability test - assert overriden rounding.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/availability.json",
        low_number_suppression=30,
        expected_count=40,
    ),
    # Basic availability test - assert low number suppression on threshold.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/availability.json",
        low_number_suppression=40,
        expected_count=40,
    ),
    # Basic availability test - assert rounding and low number suppression.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/availability.json",
        rounding=10,
        low_number_suppression=20,
        expected_count=40,
    ),
    # Basic availability test - assert rounding to 100.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/availability.json",
        rounding=100,
        expected_count=0,
    ),
    # Basic availability test - assert rounding to 10.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/availability.json",
        rounding=10,
        expected_count=40,
    ),
    # Basic gender test - assert gender OR filtering with default rounding.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/basic_gender_or.json",
        expected_count=100,
    ),
    # Basic gender test - assert gender OR filtering with rounding to 0.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/basic_gender_or.json",
        rounding=0,
        expected_count=99,
    ),
    # Multiple in group test - assert multiple in group AND filtering with default rounding.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/multiple_in_group_and.json",
        expected_count=0,
    ),
    # Mutiple in group test - assert multiple in group OR filtering with default rounding.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/multiple_in_group_or.json",
        expected_count=60,
    ),
    # Mutiple in group test - assert multiple in group OR filtering with rounding to 0.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/multiple_in_group_or.json",
        rounding=0,
        expected_count=55,
    ),
    # Mutiple in group test - assert multiple in group OR filtering with age 1.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/multiple_in_group_or_with_age1.json",
        expected_count=60,
    ),
    # Mutiple in group test - assert multiple in group OR filtering with age 1 and rounding to 0.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/multiple_in_group_or_with_age1.json",
        rounding=0,
        expected_count=55,
    ),
    # Mutiple in group test - assert multiple in group OR filtering with age 2.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/multiple_in_group_or_with_age2.json",
        expected_count=60,
    ),
    # Mutiple in group test - assert multiple in group OR filtering with age 2 and rounding to 0.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/multiple_in_group_or_with_age2.json",
        rounding=0,
        expected_count=55,
    ),
    # Basic measurement test - assert measurement with default rounding.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/measurement.json",
        rounding=0,
        low_number_suppression=10,
        expected_count=12,
    ),
    # Multiple in group, exclusion criteria, and time test.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/multiple_in_group_exclusion_time.json",
        rounding=0,
        low_number_suppression=10,
        expected_count=13,
    ),
    # Basic ethnicity test - assert ethnicity OR filtering with override rounding.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/basic_ethnicity_or.json",
        rounding=0,
        low_number_suppression=0,
        expected_count=41,
    ),
    # Basic race test - assert race OR filtering with override rounding.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/basic_race_or.json",
        rounding=0,
        low_number_suppression=0,
        expected_count=95,
    ),
    # Secondary modifiers test - assert secondary modifiers with override rounding.
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/secondary_modifiers.json",
        rounding=0,
        low_number_suppression=0,
        expected_count=13,
    ),
]
