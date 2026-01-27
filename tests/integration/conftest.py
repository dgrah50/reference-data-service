"""Integration test configuration."""

import pytest


def pytest_configure(config):
    """Register custom markers for pytest."""
    config.addinivalue_line("markers", "integration: mark test as integration test")


@pytest.fixture
def integration_timeout():
    """
    Timeout for integration tests in seconds.

    Returns:
        Timeout value in seconds for integration tests
    """
    return 30
