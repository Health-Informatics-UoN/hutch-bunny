import pytest
import os

import dotenv

from hutch_bunny.core.solvers.query_solvers import solve_availability, solve_distribution

dotenv.load_dotenv()

pytestmark = pytest.mark.skipif(
    os.environ.get("CI") is not None, reason="Skip integration tests in CI"
)


def test_pool_clean_up_availability(
    db_manager,
    availability_query_onerule_equals,
    availability_query_onerule_notequals,
    availability_query_tworules_equals,
    availability_query_tworules_notequals,
):
    starting_checked_out_connections = db_manager.engine.pool.checkedout()
    solve_availability(db_manager=db_manager, query=availability_query_onerule_equals)
    ending_checked_out_connections = db_manager.engine.pool.checkedout()
    assert starting_checked_out_connections == ending_checked_out_connections

    starting_checked_out_connections = db_manager.engine.pool.checkedout()
    solve_availability(
        db_manager=db_manager, query=availability_query_onerule_notequals
    )
    ending_checked_out_connections = db_manager.engine.pool.checkedout()
    assert starting_checked_out_connections == ending_checked_out_connections

    starting_checked_out_connections = db_manager.engine.pool.checkedout()
    solve_availability(db_manager=db_manager, query=availability_query_tworules_equals)
    ending_checked_out_connections = db_manager.engine.pool.checkedout()
    assert starting_checked_out_connections == ending_checked_out_connections

    starting_checked_out_connections = db_manager.engine.pool.checkedout()
    solve_availability(
        db_manager=db_manager, query=availability_query_tworules_notequals
    )
    ending_checked_out_connections = db_manager.engine.pool.checkedout()
    assert starting_checked_out_connections == ending_checked_out_connections


def test_pool_clean_up_distribution(db_manager, distribution_query):
    starting_checked_out_connections = db_manager.engine.pool.checkedout()
    solve_distribution(db_manager=db_manager, query=distribution_query)
    ending_checked_out_connections = db_manager.engine.pool.checkedout()
    assert starting_checked_out_connections == ending_checked_out_connections
