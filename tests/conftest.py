"""
pytest fixtures for tersedecompress tests.

The test-data directory is resolved from the TEST_DATA_DIR environment
variable (default: /tmp/tersedecompress-testdata).
"""

import os
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def test_data_dir() -> Path:
    """Return the root directory containing TERSED/, ZOSBINARY/, ZOSTEXT/."""
    env = os.environ.get("TEST_DATA_DIR", "/tmp/tersedecompress-testdata")
    path = Path(env)
    if not path.is_dir():
        pytest.skip(
            f"Test data directory not found: {path}. "
            "Set TEST_DATA_DIR to the correct path."
        )
    return path


@pytest.fixture(scope="session")
def tersed_dir(test_data_dir: Path) -> Path:
    return test_data_dir / "TERSED"


@pytest.fixture(scope="session")
def binary_dir(test_data_dir: Path) -> Path:
    return test_data_dir / "ZOSBINARY"


@pytest.fixture(scope="session")
def text_dir(test_data_dir: Path) -> Path:
    return test_data_dir / "ZOSTEXT"
