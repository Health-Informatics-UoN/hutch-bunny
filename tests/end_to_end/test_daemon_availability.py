from typing import Generator
import pytest
import os
import json
import time
from pytest_httpserver import HTTPServer
import threading
from werkzeug.wrappers import Request, Response

from hutch_bunny.daemon import main as daemon_main
from hutch_bunny.core.settings import get_settings, DaemonSettings


class DaemonRunner(threading.Thread):
    """Thread class to run the daemon in the background using the actual daemon entrypoint."""

    def __init__(self) -> None:
        threading.Thread.__init__(self)
        self.daemon = True  # Set thread as daemon so it dies when main thread exits

    def run(self) -> None:
        # Patch the polling service to use max_iterations=1
        from hutch_bunny.core.upstream.polling_service import PollingService

        original_poll = PollingService.poll_for_tasks

        def patched_poll(
            self: PollingService, max_iterations: int | None = None
        ) -> None:
            return original_poll(self, max_iterations=1)

        PollingService.poll_for_tasks = patched_poll  # type: ignore

        # Use the actual daemon entrypoint
        daemon_main()


@pytest.fixture
def mock_daemon_settings(
    httpserver: HTTPServer,
) -> Generator[DaemonSettings, None, None]:
    """Create test settings for the daemon."""

    os.environ["TASK_API_BASE_URL"] = httpserver.url_for("").rstrip("/")
    os.environ["COLLECTION_ID"] = "collection_id"
    os.environ["TASK_API_TYPE"] = "a"

    yield get_settings(daemon=True)


@pytest.mark.end_to_end
def test_daemon_availability(
    httpserver: HTTPServer, mock_daemon_settings: DaemonSettings
) -> None:
    """
    Test the daemon's ability to process a task.

    This test starts a mock HTTP server that:
    1. Responds to the daemon's polling request with a task
    2. Accepts the results from the daemon

    Args:
        httpserver: Pytest fixture that provides a local HTTP server
        mock_daemon_settings: Custom settings for the daemon
    """
    # Load test query
    with open("tests/queries/availability/availability.json", "r") as f:
        task_data = json.load(f)

    # Configure the mock server to respond to the nextjob request with task first, then no tasks
    endpoint = f"/task/nextjob/{mock_daemon_settings.COLLECTION_ID}.{mock_daemon_settings.TASK_API_TYPE}"

    # Queue of responses: first real task, then no content
    responses = [
        (json.dumps(task_data), 200, {"Content-Type": "application/json"}),
        ("", 204, {"Content-Type": "application/json"}),
    ]
    response_index = 0

    def next_response(request: Request) -> Response:
        nonlocal response_index
        if response_index < len(responses):
            content, status, headers = responses[response_index]
            response_index += 1
            return Response(content, status=status, headers=headers)
        return Response(
            "", status=204
        )  # Default to no content after queue is exhausted

    httpserver.expect_request(endpoint).respond_with_handler(next_response)

    # Configure the mock server to handle the results
    results_received = {}

    def store_results(request: Request) -> Response:
        results_received.update(json.loads(request.data))
        return Response(status=200)

    result_endpoint = f"/task/result/{task_data['uuid']}/{task_data['collection']}"
    httpserver.expect_request(result_endpoint, method="POST").respond_with_handler(
        store_results
    )

    # Start the daemon in a separate thread
    daemon_thread = DaemonRunner()
    daemon_thread.start()

    # Wait for results to be received or timeout
    start_time = time.time()
    timeout = 10
    while not results_received and time.time() - start_time < timeout:
        time.sleep(0.1)

    # Verify the request was processed and results were sent back
    assert "status" in results_received, "No results received from daemon"
    assert "protocolVersion" in results_received
    assert "uuid" in results_received
    assert "queryResult" in results_received
    assert "count" in results_received["queryResult"]

    # Assert expected values
    assert results_received["status"] == "ok"
    assert results_received["protocolVersion"] == "v2"
    assert results_received["uuid"] == "unique_id"
    assert results_received["queryResult"]["count"] == 40
    assert results_received["collection_id"] == "collection_id"
