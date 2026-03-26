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
# Helper
# ---------------------------------------------------------------------------


def _decompress(tersed_path: Path, *, text_mode: bool) -> bytes:
    """Decompress *tersed_path* and return the raw output bytes."""
    out = io.BytesIO()
    with tersed_path.open("rb") as f:
        with TerseDecompresser.create(f, out) as d:
            d.text_flag = text_mode
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
# PACK binary tests (36 tests)
# ---------------------------------------------------------------------------


class TestBinaryPack:
    def test_pack_binary_01(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.A.TXT", "PACK")

    def test_pack_binary_02(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.AAA.TXT", "PACK")

    def test_pack_binary_03(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.ALICE29.TXT", "PACK")

    def test_pack_binary_04(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.ALPHABET.TXT", "PACK")

    def test_pack_binary_05(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.ASYOULIK.TXT", "PACK")

    def test_pack_binary_06(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.BIBLE.TXT", "PACK")

    def test_pack_binary_07(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.CP.HTML", "PACK")

    def test_pack_binary_08(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.E.COLI", "PACK")

    def test_pack_binary_09(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.FIELDS.C", "PACK")

    def test_pack_binary_10(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.GRAMMAR.LSP", "PACK")

    def test_pack_binary_11(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.KENNEDY.XLS", "PACK")

    def test_pack_binary_12(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.LCET10.TXT", "PACK")

    def test_pack_binary_13(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.PI.TXT", "PACK")

    def test_pack_binary_14(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.PLRABN12.TXT", "PACK")

    def test_pack_binary_15(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.PTT5", "PACK")

    def test_pack_binary_16(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.RANDOM.TXT", "PACK")

    def test_pack_binary_17(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.SUM", "PACK")

    def test_pack_binary_18(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.WORLD192.TXT", "PACK")

    def test_pack_binary_19(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.XARGS", "PACK")

    def test_pack_binary_20(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.BIBLE.TXT", "PACK")

    def test_pack_binary_21(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.CP.HTML", "PACK")

    def test_pack_binary_22(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.ENWIK8.XML", "PACK")

    def test_pack_binary_23(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.FIELDS.C", "PACK")

    def test_pack_binary_24(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.GRAMMAR.LSP", "PACK")

    def test_pack_binary_25(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.LCET10.TXT", "PACK")

    def test_pack_binary_26(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.WORLD192.TXT", "PACK")

    def test_pack_binary_27(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.XARGS", "PACK")

    def test_pack_binary_28(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.A.TXT", "PACK")

    def test_pack_binary_29(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.AAA.TXT", "PACK")

    def test_pack_binary_30(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.ALPHABET.TXT", "PACK")

    def test_pack_binary_31(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.E.COLI", "PACK")

    def test_pack_binary_32(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.PI.TXT", "PACK")

    def test_pack_binary_33(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.RANDOM.TXT", "PACK")

    def test_pack_binary_34(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.ALICE29.TXT", "PACK")

    def test_pack_binary_35(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.ASYOULIK.TXT", "PACK")

    def test_pack_binary_36(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.PLRABN12.TXT", "PACK")


# ---------------------------------------------------------------------------
# SPACK binary tests (35 tests — FB.A.TXT and VB.A.TXT are known-broken)
# ---------------------------------------------------------------------------


class TestBinarySpack:
    # FB.A.TXT.SPACK — known-broken (AMATERSE compression artefact on z/OS)
    @pytest.mark.skip(reason="Known-broken: AMATERSE SPACK compression artefact")
    def test_spack_binary_01(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.A.TXT", "SPACK")

    def test_spack_binary_02(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.AAA.TXT", "SPACK")

    def test_spack_binary_03(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.ALICE29.TXT", "SPACK")

    def test_spack_binary_04(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.ALPHABET.TXT", "SPACK")

    def test_spack_binary_05(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.ASYOULIK.TXT", "SPACK")

    def test_spack_binary_06(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.BIBLE.TXT", "SPACK")

    def test_spack_binary_07(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.CP.HTML", "SPACK")

    def test_spack_binary_08(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.E.COLI", "SPACK")

    def test_spack_binary_09(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.FIELDS.C", "SPACK")

    def test_spack_binary_10(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.GRAMMAR.LSP", "SPACK")

    def test_spack_binary_11(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.KENNEDY.XLS", "SPACK")

    def test_spack_binary_12(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.LCET10.TXT", "SPACK")

    def test_spack_binary_13(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.PI.TXT", "SPACK")

    def test_spack_binary_14(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.PLRABN12.TXT", "SPACK")

    def test_spack_binary_15(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.PTT5", "SPACK")

    def test_spack_binary_16(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.RANDOM.TXT", "SPACK")

    def test_spack_binary_17(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.SUM", "SPACK")

    def test_spack_binary_18(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.WORLD192.TXT", "SPACK")

    def test_spack_binary_19(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "FB.XARGS", "SPACK")

    def test_spack_binary_20(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.BIBLE.TXT", "SPACK")

    def test_spack_binary_21(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.CP.HTML", "SPACK")

    def test_spack_binary_22(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.ENWIK8.XML", "SPACK")

    def test_spack_binary_23(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.FIELDS.C", "SPACK")

    def test_spack_binary_24(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.GRAMMAR.LSP", "SPACK")

    def test_spack_binary_25(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.LCET10.TXT", "SPACK")

    def test_spack_binary_26(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.WORLD192.TXT", "SPACK")

    def test_spack_binary_27(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.XARGS", "SPACK")

    # VB.A.TXT.SPACK — known-broken (AMATERSE compression artefact on z/OS)
    @pytest.mark.skip(reason="Known-broken: AMATERSE SPACK compression artefact")
    def test_spack_binary_28(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.A.TXT", "SPACK")

    def test_spack_binary_29(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.AAA.TXT", "SPACK")

    def test_spack_binary_30(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.ALPHABET.TXT", "SPACK")

    def test_spack_binary_31(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.E.COLI", "SPACK")

    def test_spack_binary_32(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.PI.TXT", "SPACK")

    def test_spack_binary_33(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.RANDOM.TXT", "SPACK")

    def test_spack_binary_34(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.ALICE29.TXT", "SPACK")

    def test_spack_binary_35(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.ASYOULIK.TXT", "SPACK")

    def test_spack_binary_36(self, tersed_dir, binary_dir):
        _run_binary_test(tersed_dir, binary_dir, "VB.PLRABN12.TXT", "SPACK")


# ---------------------------------------------------------------------------
# PACK text tests (26 tests)
# ---------------------------------------------------------------------------


class TestTextPack:
    def test_pack_text_01(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.A.TXT", "PACK")

    def test_pack_text_02(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.AAA.TXT", "PACK")

    def test_pack_text_04(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.ALPHABET.TXT", "PACK")

    def test_pack_text_06(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.BIBLE.TXT", "PACK")

    def test_pack_text_07(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.CP.HTML", "PACK")

    def test_pack_text_08(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.E.COLI", "PACK")

    def test_pack_text_09(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.FIELDS.C", "PACK")

    def test_pack_text_10(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.GRAMMAR.LSP", "PACK")

    def test_pack_text_12(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.LCET10.TXT", "PACK")

    def test_pack_text_13(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.PI.TXT", "PACK")

    def test_pack_text_16(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.RANDOM.TXT", "PACK")

    def test_pack_text_18(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.WORLD192.TXT", "PACK")

    def test_pack_text_19(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.XARGS", "PACK")

    def test_pack_text_20(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.BIBLE.TXT", "PACK")

    def test_pack_text_21(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.CP.HTML", "PACK")

    def test_pack_text_23(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.FIELDS.C", "PACK")

    def test_pack_text_24(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.GRAMMAR.LSP", "PACK")

    def test_pack_text_25(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.LCET10.TXT", "PACK")

    def test_pack_text_26(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.WORLD192.TXT", "PACK")

    def test_pack_text_27(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.XARGS", "PACK")

    def test_pack_text_28(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.A.TXT", "PACK")

    def test_pack_text_29(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.AAA.TXT", "PACK")

    def test_pack_text_30(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.ALPHABET.TXT", "PACK")

    def test_pack_text_31(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.E.COLI", "PACK")

    def test_pack_text_32(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.PI.TXT", "PACK")

    def test_pack_text_33(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.RANDOM.TXT", "PACK")


# ---------------------------------------------------------------------------
# SPACK text tests (25 tests — FB.A.TXT and VB.A.TXT are known-broken)
# ---------------------------------------------------------------------------


class TestTextSpack:
    # FB.A.TXT.SPACK — known-broken
    @pytest.mark.skip(reason="Known-broken: AMATERSE SPACK compression artefact")
    def test_spack_text_01(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.A.TXT", "SPACK")

    def test_spack_text_02(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.AAA.TXT", "SPACK")

    def test_spack_text_04(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.ALPHABET.TXT", "SPACK")

    def test_spack_text_06(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.BIBLE.TXT", "SPACK")

    def test_spack_text_07(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.CP.HTML", "SPACK")

    def test_spack_text_08(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.E.COLI", "SPACK")

    def test_spack_text_09(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.FIELDS.C", "SPACK")

    def test_spack_text_10(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.GRAMMAR.LSP", "SPACK")

    def test_spack_text_12(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.LCET10.TXT", "SPACK")

    def test_spack_text_13(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.PI.TXT", "SPACK")

    def test_spack_text_16(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.RANDOM.TXT", "SPACK")

    def test_spack_text_18(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.WORLD192.TXT", "SPACK")

    def test_spack_text_19(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "FB.XARGS", "SPACK")

    def test_spack_text_20(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.BIBLE.TXT", "SPACK")

    def test_spack_text_21(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.CP.HTML", "SPACK")

    def test_spack_text_23(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.FIELDS.C", "SPACK")

    def test_spack_text_24(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.GRAMMAR.LSP", "SPACK")

    def test_spack_text_25(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.LCET10.TXT", "SPACK")

    def test_spack_text_26(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.WORLD192.TXT", "SPACK")

    def test_spack_text_27(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.XARGS", "SPACK")

    # VB.A.TXT.SPACK — known-broken
    @pytest.mark.skip(reason="Known-broken: AMATERSE SPACK compression artefact")
    def test_spack_text_28(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.A.TXT", "SPACK")

    def test_spack_text_29(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.AAA.TXT", "SPACK")

    def test_spack_text_30(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.ALPHABET.TXT", "SPACK")

    def test_spack_text_31(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.E.COLI", "SPACK")

    def test_spack_text_32(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.PI.TXT", "SPACK")

    def test_spack_text_33(self, tersed_dir, text_dir):
        _run_text_test(tersed_dir, text_dir, "VB.RANDOM.TXT", "SPACK")
