"""
Integration tests for tersedecompress.

All test cases are ported directly from AppTest.java.

Binary tests (testBinary): decompress TERSED/{file}.{algo} with
  text_flag=False (binary mode) and compare to ZOSBINARY/{file}.

Text tests (testText): decompress TERSED/{file}.{algo} with
  text_flag=True (text mode) and compare to ZOSTEXT/{file}.

Known-broken files (AMATERSE compression artefacts on z/OS):
  - FB.A.TXT.SPACK  (binary + text)
  - VB.A.TXT.SPACK  (binary + text)
These are intentionally skipped, matching the Java test suite.
"""

import io
from pathlib import Path

import pytest

from tersedecompress.base import TerseDecompresser


# ---------------------------------------------------------------------------
# File lists (Issues #15 / #17)
# ---------------------------------------------------------------------------

FB_FILES = [
    "FB.A.TXT",
    "FB.AAA.TXT",
    "FB.ALICE29.TXT",
    "FB.ALPHABET.TXT",
    "FB.ASYOULIK.TXT",
    "FB.BIBLE.TXT",
    "FB.CP.HTML",
    "FB.E.COLI",
    "FB.FIELDS.C",
    "FB.GRAMMAR.LSP",
    "FB.KENNEDY.XLS",
    "FB.LCET10.TXT",
    "FB.PI.TXT",
    "FB.PLRABN12.TXT",
    "FB.PTT5",
    "FB.RANDOM.TXT",
    "FB.SUM",
    "FB.WORLD192.TXT",
    "FB.XARGS",
]

VB_FILES = [
    "VB.BIBLE.TXT",
    "VB.CP.HTML",
    "VB.ENWIK8.XML",
    "VB.FIELDS.C",
    "VB.GRAMMAR.LSP",
    "VB.LCET10.TXT",
    "VB.WORLD192.TXT",
    "VB.XARGS",
    "VB.A.TXT",
    "VB.AAA.TXT",
    "VB.ALPHABET.TXT",
    "VB.E.COLI",
    "VB.PI.TXT",
    "VB.RANDOM.TXT",
    "VB.ALICE29.TXT",
    "VB.ASYOULIK.TXT",
    "VB.PLRABN12.TXT",
]

ALL_FILES = FB_FILES + VB_FILES

# Files only present in binary test suite (no text counterpart in ZOSTEXT)
BINARY_ONLY_FILES = {
    "FB.ALICE29.TXT",
    "FB.ASYOULIK.TXT",
    "FB.KENNEDY.XLS",
    "FB.PLRABN12.TXT",
    "FB.PTT5",
    "FB.SUM",
    "VB.ENWIK8.XML",
    "VB.ALICE29.TXT",
    "VB.ASYOULIK.TXT",
    "VB.PLRABN12.TXT",
}

# Known-broken in AMATERSE SPACK (skip for both binary and text)
SPACK_SKIP = {"FB.A.TXT", "VB.A.TXT"}


# ---------------------------------------------------------------------------
# Helpers (Issues #2 / #17)
# ---------------------------------------------------------------------------


def _decompress(tersed_path: Path, *, text_mode: bool) -> bytes:
    """Decompress *tersed_path* and return the raw output bytes."""
    out = io.BytesIO()
    with tersed_path.open("rb") as f:
        with TerseDecompresser.create(f, out, text_mode=text_mode) as d:
            d.decode()
    return out.getvalue()


def _run_binary_test(tersed_dir: Path, binary_dir: Path, file: str, algo: str) -> None:
    tersed_path = tersed_dir / f"{file}.{algo}"
    expected_path = binary_dir / file
    expected = expected_path.read_bytes()
    got = _decompress(tersed_path, text_mode=False)
    assert got == expected, f"Binary mismatch for {file}.{algo}"


def _run_text_test(tersed_dir: Path, text_dir: Path, file: str, algo: str) -> None:
    tersed_path = tersed_dir / f"{file}.{algo}"
    expected_path = text_dir / file
    expected = expected_path.read_bytes()
    got = _decompress(tersed_path, text_mode=True)
    assert got == expected, f"Text mismatch for {file}.{algo}"


# ---------------------------------------------------------------------------
# PACK binary tests — parametrized (Issue #15)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("filename", ALL_FILES)
def test_pack_binary(tersed_dir: Path, binary_dir: Path, filename: str) -> None:
    _run_binary_test(tersed_dir, binary_dir, filename, "PACK")


# ---------------------------------------------------------------------------
# SPACK binary tests — parametrized, known-broken skipped (Issue #15)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename",
    [
        pytest.param(
            f,
            marks=pytest.mark.skip(
                reason="Known-broken: AMATERSE SPACK compression artefact"
            ),
        )
        if f in SPACK_SKIP
        else f
        for f in ALL_FILES
    ],
)
def test_spack_binary(tersed_dir: Path, binary_dir: Path, filename: str) -> None:
    _run_binary_test(tersed_dir, binary_dir, filename, "SPACK")


# ---------------------------------------------------------------------------
# PACK text tests — parametrized, binary-only files skipped (Issue #15)
# ---------------------------------------------------------------------------

TEXT_FILES = [f for f in ALL_FILES if f not in BINARY_ONLY_FILES]


@pytest.mark.parametrize("filename", TEXT_FILES)
def test_pack_text(tersed_dir: Path, text_dir: Path, filename: str) -> None:
    _run_text_test(tersed_dir, text_dir, filename, "PACK")


# ---------------------------------------------------------------------------
# SPACK text tests — parametrized, known-broken + binary-only skipped (Issue #15)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename",
    [
        pytest.param(
            f,
            marks=pytest.mark.skip(
                reason="Known-broken: AMATERSE SPACK compression artefact"
            ),
        )
        if f in SPACK_SKIP
        else f
        for f in TEXT_FILES
    ],
)
def test_spack_text(tersed_dir: Path, text_dir: Path, filename: str) -> None:
    _run_text_test(tersed_dir, text_dir, filename, "SPACK")
