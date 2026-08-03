from enum import StrEnum


class Varcat(StrEnum):
    """All values a `Rule.varcat` may take."""

    PERSON = "Person"
    CONDITION = "Condition"
    OBSERVATION = "Observation"
    DRUG = "Drug"
    MEASUREMENT = "Measurement"
    MEDICATION = "Medication"
    PROCEDURE = "Procedure"
    SPECIMEN = "Specimen"
    LOCATION = "Location"
