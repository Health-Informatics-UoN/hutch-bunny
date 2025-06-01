from sqlalchemy import Engine, select
from hutch_bunny.core.entities import Concept

from hutch_bunny.core.rquest_dto.group import Group


class ConceptService:
    def __init__(self, db_engine: Engine):
        self.db_engine = db_engine

    def get_concept_domains(self, groups: list[Group]) -> dict[str, str]:
        """Look up concept_id → domain_id mappings for concept IDs in the cohort definition.

        This is more robust than trusting the payload-supplied domain, since concepts can move across domains
        between vocab versions.
        """
        concept_ids = {
            int(rule.value) for group in groups for rule in group.rules if rule.value
        }

        if not concept_ids:
            return {}

        concept_query = (
            select(Concept.concept_id, Concept.domain_id)
            .where(Concept.concept_id.in_(concept_ids))
            .distinct()
        )

        with self.db_engine.connect() as con:
            result = con.execute(concept_query)
            return {str(concept_id): domain_id for concept_id, domain_id in result}
