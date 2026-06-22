"""
pytest fixtures for tersedecompress tests.

The test-data directory is resolved from the TEST_DATA_DIR environment
variable (default: /tmp/tersedecompress-testdata).

Shared fixtures for test_file_api.py, test_streaming.py, and others:
- small_pack_file    — FB.AAA.TXT.PACK (smallest PACK file)
- pack_bytes         — raw compressed bytes of small_pack_file
- expected_binary    — expected binary decompression output
- expected_text      — expected text-mode decompression output
"""

import os
from pathlib import Path

import pytest

from tersedecompress.core import decompress


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


# ---------------------------------------------------------------------------
# Shared file fixtures (used by test_file_api.py and test_streaming.py)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def small_pack_file(test_data_dir: Path) -> Path:
    """Return path to FB.AAA.TXT.PACK — the smallest PACK test file."""
    p = test_data_dir / "TERSED" / "FB.AAA.TXT.PACK"
    if not p.exists():
        pytest.skip(f"Test file not found: {p}")
    return p


@pytest.fixture(scope="session")
def pack_bytes(small_pack_file: Path) -> bytes:
    """Raw compressed bytes of FB.AAA.TXT.PACK."""
    return small_pack_file.read_bytes()


@pytest.fixture(scope="session")
def expected_binary(pack_bytes: bytes) -> bytes:
    """Expected binary decompression output of FB.AAA.TXT.PACK."""
    return decompress(pack_bytes, text_mode=False)


@pytest.fixture(scope="session")
def expected_text(pack_bytes: bytes) -> bytes:
    """Expected text-mode decompression output of FB.AAA.TXT.PACK."""
    return decompress(pack_bytes, text_mode=True)
