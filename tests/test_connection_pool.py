import pytest
import os

from hutch_bunny.core.db_manager import SyncDBManager
import hutch_bunny.core.settings as settings
from hutch_bunny.core.query_solvers import (
    AvailabilityQuery,
    DistributionQuery,
    solve_availability,
    solve_distribution,
)
from hutch_bunny.core.rquest_dto.cohort import Cohort
from hutch_bunny.core.rquest_dto.group import Group
from hutch_bunny.core.rquest_dto.rule import Rule

@pytest.fixture
def db_manager():
    datasource_db_port = os.getenv("DATASOURCE_DB_PORT")
    return SyncDBManager(
        username=os.getenv("DATASOURCE_DB_USERNAME"),
        password=os.getenv("DATASOURCE_DB_PASSWORD"),
        host=os.getenv("DATASOURCE_DB_HOST"),
        port=(int(datasource_db_port) if datasource_db_port is not None else None),
        database=os.getenv("DATASOURCE_DB_DATABASE"),
        drivername=os.getenv("DATASOURCE_DB_DRIVERNAME", settings.DEFAULT_DB_DRIVER),
        schema=os.getenv("DATASOURCE_DB_SCHEMA"),
    )

@pytest.fixture
def availability_query_equals():
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
def availability_query_notequals():
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
def availability_query_2ndrule_equals():
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
                            operator="=",
                            value="28060",
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
def availability_query_2ndrule_notequals():
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
def distribution_query():
    return DistributionQuery(
        owner="user1",
        code="DEMOGRAPHICS",
        analysis="DISTRIBUTION",
        uuid="unique_id",
        collection="collection_id",
    )

def test_pool_clean_up_availability(db_manager,availability_query_equals,availability_query_notequals,
                                    availability_query_2ndrule_equals,availability_query_2ndrule_notequals):
    starting_checked_out_connections = db_manager.engine.pool.checkedout()
    solve_availability(db_manager=db_manager, query=availability_query_equals)
    ending_checked_out_connections = db_manager.engine.pool.checkedout()
    assert starting_checked_out_connections==ending_checked_out_connections

    starting_checked_out_connections = db_manager.engine.pool.checkedout()
    solve_availability(db_manager=db_manager, query=availability_query_notequals)
    ending_checked_out_connections = db_manager.engine.pool.checkedout()
    assert starting_checked_out_connections==ending_checked_out_connections

    starting_checked_out_connections = db_manager.engine.pool.checkedout()
    solve_availability(db_manager=db_manager, query=availability_query_2ndrule_equals)
    ending_checked_out_connections = db_manager.engine.pool.checkedout()
    assert starting_checked_out_connections==ending_checked_out_connections

    starting_checked_out_connections = db_manager.engine.pool.checkedout()
    solve_availability(db_manager=db_manager, query=availability_query_2ndrule_notequals)
    ending_checked_out_connections = db_manager.engine.pool.checkedout()
    assert starting_checked_out_connections==ending_checked_out_connections

def test_pool_clean_up_distribution(db_manager, distribution_query):
    starting_checked_out_connections = db_manager.engine.pool.checkedout()
    solve_distribution(db_manager=db_manager, query=distribution_query)
    ending_checked_out_connections = db_manager.engine.pool.checkedout()
    assert starting_checked_out_connections==ending_checked_out_connections
