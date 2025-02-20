import pytest
import os

from hutch_bunny.core.rquest_dto.cohort import Cohort
from hutch_bunny.core.rquest_dto.group import Group
from hutch_bunny.core.rquest_dto.rule import Rule
from hutch_bunny.core.db_manager import SyncDBManager
from hutch_bunny.core.solvers.query_solvers import AvailabilityQuery, DistributionQuery
from hutch_bunny.core.settings import get_settings
import hutch_bunny.core.setting_database as db_settings


settings = get_settings()

@pytest.fixture
def db_manager():
    datasource_db_port = os.getenv("DATASOURCE_DB_PORT")
    return SyncDBManager(
        username=settings.DATASOURCE_DB_USERNAME,
        password=settings.DATASOURCE_DB_PASSWORD,
        host=settings.DATASOURCE_DB_HOST,
        port=(int(datasource_db_port) if datasource_db_port is not None else None),
        database=settings.DATASOURCE_DB_DATABASE,
        drivername=db_settings.expand_short_drivers(
            settings.DEFAULT_DB_DRIVER
        ),
        schema=settings.DATASOURCE_DB_SCHEMA,
    )


@pytest.fixture
def availability_query_onerule_equals():
    return AvailabilityQuery(
        cohort=Cohort(
            [
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
def availability_query_onerule_notequals():
    return AvailabilityQuery(
        cohort=Cohort(
            [
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
def availability_query_tworules_equals():
    return AvailabilityQuery(
        cohort=Cohort(
            [
                Group(
                    rules=[
                        Rule(
                            varname="OMOP",
                            varcat="Person",
                            type_="TEXT",
                            operator="==",
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
def availability_query_tworules_notequals():
    return AvailabilityQuery(
        cohort=Cohort(
            [
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
def distribution_query():
    return DistributionQuery(
        owner="user1",
        code="DEMOGRAPHICS",
        analysis="DISTRIBUTION",
        uuid="unique_id",
        collection="collection_id",
    )
