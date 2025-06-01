from sqlalchemy import Engine, select
from hutch_bunny.core.entities import Concept
from typing import TypeAlias

from hutch_bunny.core.rquest_dto.group import Group


ConceptDomainMap: TypeAlias = dict[str, str]


class ConceptService:
    def __init__(self, db_engine: Engine):
        self.db_engine = db_engine

    def map_concepts_to_domains(self, groups: list[Group]) -> ConceptDomainMap:
        """
        Map concept IDs to their domain IDs.
        This is more robust than trusting the payload-supplied domain, since concepts can move across domains
        between vocab versions.

        Args:
            groups (list[Group]): The groups to map concepts to domains.

        Returns:
            ConceptDomainMap: A dictionary mapping concept IDs to their domain IDs.
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
