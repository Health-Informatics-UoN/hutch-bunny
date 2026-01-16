import pytest
from hutch_bunny.core.rquest_models.cohort import Cohort
from hutch_bunny.core.rquest_models.group import Group
from hutch_bunny.core.rquest_models.rule import Rule
from hutch_bunny.core.db import BaseDBClient
from hutch_bunny.core.rquest_models.distribution import (
    DistributionQuery,
    DistributionQueryType,
)
from hutch_bunny.core.rquest_models.availability import AvailabilityQuery
from hutch_bunny.core.settings import Settings
from hutch_bunny.core.db import get_db_client


settings = Settings()


@pytest.fixture
def db_client() -> BaseDBClient:
    return get_db_client()

@pytest.fixture
def availability_query_onerule_equals() -> AvailabilityQuery:
    return AvailabilityQuery(
        cohort=Cohort(
            groups=[
                Group(
                    rules=[
                        Rule(
                            varname="OMOP",
                            varcat="Person",
                            type_="TEXT",
                            operator="=",
                            value="8507",
                        )
                    ],
                    rules_operator="AND",
                ),
            ],
            groups_operator="OR",
        ),
        uuid="unique_id",
        protocol_version="v2",
        char_salt="salt",
        collection="collection_id",
        owner="user1",
    )


@pytest.fixture
def availability_query_onerule_notequals() -> AvailabilityQuery:
    return AvailabilityQuery(
        cohort=Cohort(
            groups=[
                Group(
                    rules=[
                        Rule(
                            varname="OMOP",
                            varcat="Person",
                            type_="TEXT",
                            operator="!=",
                            value="8507",
                        )
                    ],
                    rules_operator="AND",
                ),
            ],
            groups_operator="OR",
        ),
        uuid="unique_id",
        protocol_version="v2",
        char_salt="salt",
        collection="collection_id",
        owner="user1",
    )


@pytest.fixture
def availability_query_tworules_equals() -> AvailabilityQuery:
    return AvailabilityQuery(
        cohort=Cohort(
            groups=[
                Group(
                    rules=[
                        Rule(
                            varname="OMOP",
                            varcat="Person",
                            type_="TEXT",
                            operator="=",
                            value="8507",
                        ),
                        Rule(
                            varname="OMOP",
                            varcat="Condition",
                            type_="TEXT",
                            operator="=",
                            value="28060",
                        ),
                    ],
                    rules_operator="AND",
                ),
            ],
            groups_operator="OR",
        ),
        uuid="unique_id",
        protocol_version="v2",
        char_salt="salt",
        collection="collection_id",
        owner="user1",
    )


@pytest.fixture
def availability_query_tworules_notequals() -> AvailabilityQuery:
    return AvailabilityQuery(
        cohort=Cohort(
            groups=[
                Group(
                    rules=[
                        Rule(
                            varname="OMOP",
                            varcat="Person",
                            type_="TEXT",
                            operator="!=",
                            value="8507",
                        ),
                        Rule(
                            varname="OMOP",
                            varcat="Condition",
                            type_="TEXT",
                            operator="!=",
                            value="28060",
                        ),
                    ],
                    rules_operator="AND",
                ),
            ],
            groups_operator="OR",
        ),
        uuid="unique_id",
        protocol_version="v2",
        char_salt="salt",
        collection="collection_id",
        owner="user1",
    )


@pytest.fixture
def distribution_query() -> DistributionQuery:
    return DistributionQuery(
        owner="user1",
        code=DistributionQueryType.DEMOGRAPHICS,
        analysis="DISTRIBUTION",
        uuid="unique_id",
        collection="collection_id",
    )
