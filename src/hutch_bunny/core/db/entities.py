from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    ForeignKey,
    Integer,
    Numeric,
    String,
    DateTime,
    Text,
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Base class for all models."""

    pass


class Concept(Base):
    __tablename__ = "concept"
    concept_id = Column(Integer, primary_key=True)
    concept_name = Column(String(255), nullable=False)
    domain_id = Column(String(20), nullable=False)
    vocabulary_id = Column(String(20), nullable=False)
    concept_class_id = Column(String(20), nullable=False)
    standard_concept = Column(String(1), nullable=True)
    concept_code = Column(String(50), nullable=False)
    valid_start_date = Column(Date, nullable=False)
    valid_end_date = Column(Date, nullable=False)
    invalid_reason = Column(String(1), nullable=True)


class Person(Base):
    __tablename__ = "person"
    person_id = Column(Integer, primary_key=True)
    gender_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=False
    )
    year_of_birth = Column(Integer, nullable=False)
    month_of_birth = Column(Integer, nullable=True)
    day_of_birth = Column(Integer, nullable=True)
    birth_datetime = Column(DateTime, nullable=True)
    race_concept_id = Column(Integer, ForeignKey("concept.concept_id"), nullable=False)
    ethnicity_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=False
    )
    location_id = Column(Integer, nullable=True)
    provider_id = Column(Integer, nullable=True)
    care_site_id = Column(Integer, nullable=True)
    person_source_value = Column(String(50), nullable=True)
    gender_source_value = Column(String(50), nullable=True)
    gender_source_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=True
    )
    race_source_value = Column(String(50), nullable=True)
    race_source_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=True
    )
    ethnicity_source_value = Column(String(50), nullable=True)
    ethnicity_source_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=True
    )


class Measurement(Base):
    __tablename__ = "measurement"
    measurement_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, ForeignKey("person.person_id"), nullable=False)
    measurement_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=False
    )
    measurement_date = Column(Date, nullable=False)
    measurement_datetime = Column(DateTime, nullable=True)
    measurement_time = Column(String(10), nullable=True)
    measurement_type_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=False
    )
    operator_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=True
    )
    value_as_number = Column(Numeric, nullable=True)
    value_as_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=True
    )
    unit_concept_id = Column(Integer, ForeignKey("concept.concept_id"), nullable=True)
    range_low = Column(Numeric, nullable=True)
    range_high = Column(Numeric, nullable=True)
    provider_id = Column(Integer, nullable=True)
    visit_occurrence_id = Column(Integer, nullable=True)
    visit_detail_id = Column(Integer, nullable=True)
    measurement_source_value = Column(String(50), nullable=True)
    measurement_source_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=True
    )
    unit_source_value = Column(String(50), nullable=True)
    unit_source_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=True
    )
    value_source_value = Column(String(50), nullable=True)
    measurement_event_id = Column(BigInteger, nullable=True)
    meas_event_field_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=True
    )


class ConditionOccurrence(Base):
    __tablename__ = "condition_occurrence"
    condition_occurrence_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, ForeignKey("person.person_id"), nullable=False)
    condition_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=False
    )
    condition_start_date = Column(Date, nullable=False)
    condition_start_datetime = Column(DateTime, nullable=True)
    condition_end_date = Column(Date, nullable=True)
    condition_end_datetime = Column(DateTime, nullable=True)
    condition_type_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=False
    )
    condition_status_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=True
    )
    stop_reason = Column(String(20), nullable=True)
    provider_id = Column(Integer, nullable=True)
    visit_occurrence_id = Column(Integer, nullable=True)
    visit_detail_id = Column(Integer, nullable=True)
    condition_source_value = Column(String(50), nullable=True)
    condition_source_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=True
    )
    condition_status_source_value = Column(String(50), nullable=True)


class Observation(Base):
    __tablename__ = "observation"
    observation_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, ForeignKey("person.person_id"), nullable=False)
    observation_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=False
    )
    observation_date = Column(Date, nullable=False)
    observation_datetime = Column(DateTime, nullable=True)
    observation_type_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=False
    )
    value_as_number = Column(Numeric, nullable=True)
    value_as_string = Column(String(60), nullable=True)
    value_as_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=True
    )
    qualifier_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=True
    )
    unit_concept_id = Column(Integer, ForeignKey("concept.concept_id"), nullable=True)
    provider_id = Column(Integer, nullable=True)
    visit_occurrence_id = Column(Integer, nullable=True)
    visit_detail_id = Column(Integer, nullable=True)
    observation_source_value = Column(String(50), nullable=True)
    observation_source_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=True
    )
    unit_source_value = Column(String(50), nullable=True)
    qualifier_source_value = Column(String(50), nullable=True)
    value_source_value = Column(String(50), nullable=True)
    observation_event_id = Column(BigInteger, nullable=True)
    obs_event_field_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=True
    )


class ProcedureOccurrence(Base):
    __tablename__ = "procedure_occurrence"
    procedure_occurrence_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, ForeignKey("person.person_id"), nullable=False)
    procedure_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=False
    )
    procedure_date = Column(Date, nullable=False)
    procedure_datetime = Column(DateTime, nullable=True)
    procedure_end_date = Column(Date, nullable=True)
    procedure_end_datetime = Column(DateTime, nullable=True)
    procedure_type_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=False
    )
    modifier_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=True
    )
    quantity = Column(Integer, nullable=True)
    provider_id = Column(Integer, nullable=True)
    visit_occurrence_id = Column(Integer, nullable=True)
    visit_detail_id = Column(Integer, nullable=True)
    procedure_source_value = Column(String(50), nullable=True)
    procedure_source_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=True
    )
    modifier_source_value = Column(String(50), nullable=True)


class DrugExposure(Base):
    __tablename__ = "drug_exposure"
    drug_exposure_id = Column(Integer, primary_key=True)
    person_id = Column(Integer, ForeignKey("person.person_id"), nullable=False)
    drug_concept_id = Column(Integer, ForeignKey("concept.concept_id"), nullable=False)
    drug_exposure_start_date = Column(Date, nullable=False)
    drug_exposure_start_datetime = Column(DateTime, nullable=True)
    drug_exposure_end_date = Column(Date, nullable=False)
    drug_exposure_end_datetime = Column(DateTime, nullable=True)
    verbatim_end_date = Column(Date, nullable=True)
    drug_type_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=False
    )
    stop_reason = Column(String(20), nullable=True)
    refills = Column(Integer, nullable=True)
    quantity = Column(Numeric, nullable=True)
    days_supply = Column(Integer, nullable=True)
    sig = Column(Text, nullable=True)
    route_concept_id = Column(Integer, ForeignKey("concept.concept_id"), nullable=True)
    lot_number = Column(String(50), nullable=True)
    provider_id = Column(Integer, nullable=True)
    visit_occurrence_id = Column(Integer, nullable=True)
    visit_detail_id = Column(Integer, nullable=True)
    drug_source_value = Column(String(50), nullable=True)
    drug_source_concept_id = Column(
        Integer, ForeignKey("concept.concept_id"), nullable=True
    )
    route_source_value = Column(String(50), nullable=True)
    dose_unit_source_value = Column(String(50), nullable=True)
