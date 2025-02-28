from hutch_bunny.core.solvers.query_solvers import (
    solve_availability,
    solve_distribution,
)
import pytest


@pytest.mark.integration
def test_pool_clean_up_availability(
    db_manager,
    availability_query_onerule_equals,
    availability_query_onerule_notequals,
    availability_query_tworules_equals,
    availability_query_tworules_notequals,
):
    starting_checked_out_connections = db_manager.engine.pool.checkedout()
    solve_availability(
        results_modifier=[],
        db_manager=db_manager,
        query=availability_query_onerule_equals,
    )
    ending_checked_out_connections = db_manager.engine.pool.checkedout()
    assert starting_checked_out_connections == ending_checked_out_connections

    starting_checked_out_connections = db_manager.engine.pool.checkedout()
    solve_availability(
        results_modifier=[],
        db_manager=db_manager,
        query=availability_query_onerule_notequals,
    )
    ending_checked_out_connections = db_manager.engine.pool.checkedout()
    assert starting_checked_out_connections == ending_checked_out_connections

    starting_checked_out_connections = db_manager.engine.pool.checkedout()
    solve_availability(
        results_modifier=[],
        db_manager=db_manager,
        query=availability_query_tworules_equals,
    )
    ending_checked_out_connections = db_manager.engine.pool.checkedout()
    assert starting_checked_out_connections == ending_checked_out_connections

    starting_checked_out_connections = db_manager.engine.pool.checkedout()
    solve_availability(
        results_modifier=[],
        db_manager=db_manager,
        query=availability_query_tworules_notequals,
    )
    ending_checked_out_connections = db_manager.engine.pool.checkedout()
    assert starting_checked_out_connections == ending_checked_out_connections


@pytest.mark.integration
def test_pool_clean_up_distribution(db_manager, distribution_query):
    starting_checked_out_connections = db_manager.engine.pool.checkedout()
    solve_distribution(
        results_modifier=[], db_manager=db_manager, query=distribution_query
    )
    ending_checked_out_connections = db_manager.engine.pool.checkedout()
    assert starting_checked_out_connections == ending_checked_out_connections
