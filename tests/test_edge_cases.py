"""
Edge-case unit tests for tersedecompress (Issue #8).

Covers:
- Empty input (0 bytes)
- Corrupt/truncated header
- File with valid header but no payload data
"""

import io
import struct

import pytest

from tersedecompress.base import TerseDecompresser
from tersedecompress.header import TerseHeader


# ---------------------------------------------------------------------------
# Helpers to build minimal valid TERSE headers
# ---------------------------------------------------------------------------

def _build_pack_header(
    record_len1: int = 80,
    variable_flag: int = 0x00,
    flags: int = 0x00,
    ratio: int = 0x00,
    block_size: int = 0x00,
    record_len2: int = 80,
) -> bytes:
    """Build a minimal 12-byte PACK host-mode header (version 0x02)."""
    header = bytearray()
    header.append(0x02)                              # version: PACK host mode
    header.append(variable_flag)                     # variable_flag
    header += struct.pack(">H", record_len1)         # RecordLen1
    header.append(flags)                             # Flags
    header.append(ratio)                             # Ratio
    header += struct.pack(">H", block_size)          # BlockSize
    header += struct.pack(">i", record_len2)         # RecordLen2 (signed 32-bit)
    return bytes(header)                             # 12 bytes total


# ---------------------------------------------------------------------------
# Issue #8a — Empty input (0 bytes)
# ---------------------------------------------------------------------------

class TestEmptyInput:
    def test_empty_input_raises_ioerror(self) -> None:
        """A completely empty stream must raise IOError (no header at all)."""
        with pytest.raises(IOError, match="Unexpected EOF"):
            TerseHeader.check_header(io.BytesIO(b""))

    def test_empty_bytes_decompress_raises(self) -> None:
        """decompress() with empty bytes must raise IOError."""
        from tersedecompress.core import decompress

        with pytest.raises(IOError):
            decompress(b"")


# ---------------------------------------------------------------------------
# Issue #8b — Corrupt / truncated header
# ---------------------------------------------------------------------------

class TestCorruptHeader:
    def test_truncated_after_version_byte(self) -> None:
        """Stream that ends after the version byte (0x02) must raise IOError."""
        with pytest.raises(IOError, match="Unexpected EOF"):
            TerseHeader.check_header(io.BytesIO(b"\x02"))

    def test_truncated_after_variable_flag(self) -> None:
        """Stream that ends after version + variable_flag must raise IOError."""
        with pytest.raises(IOError, match="Unexpected EOF"):
            TerseHeader.check_header(io.BytesIO(b"\x02\x00"))

    def test_unknown_version_byte_raises(self) -> None:
        """An unrecognised version byte must raise IOError."""
        with pytest.raises(IOError, match="not recognized"):
            TerseHeader.check_header(io.BytesIO(b"\xFF" + b"\x00" * 11))

    def test_invalid_native_header_validation_bytes(self) -> None:
        """Native header (0x01) with wrong validation bytes must raise IOError."""
        # version=0x01, wrong validation bytes, then 2-byte RecordLen1
        data = b"\x01\x00\x00\x00\x00\x00"
        with pytest.raises(IOError, match="Invalid header validation flags"):
            TerseHeader.check_header(io.BytesIO(data))

    def test_record_length_zero_raises(self) -> None:
        """A host-mode header with both record lengths == 0 must raise IOError."""
        header_bytes = _build_pack_header(record_len1=0, record_len2=0)
        with pytest.raises(IOError, match="Record length is 0"):
            TerseHeader.check_header(io.BytesIO(header_bytes))

    def test_ambiguous_record_length_raises(self) -> None:
        """Host-mode header with conflicting record lengths must raise IOError."""
        header_bytes = _build_pack_header(record_len1=80, record_len2=100)
        with pytest.raises(IOError, match="Ambiguous record length"):
            TerseHeader.check_header(io.BytesIO(header_bytes))


# ---------------------------------------------------------------------------
# Issue #8c — Valid header but no payload data (truncated after header)
# ---------------------------------------------------------------------------

class TestHeaderOnlyNoPayload:
    def test_pack_header_only_produces_empty_output(self) -> None:
        """A PACK file with a valid header and zero payload bytes decompresses
        to empty output (the block reader returns ENDOFFILE immediately)."""
        header_bytes = _build_pack_header(record_len1=80, record_len2=80)
        out = io.BytesIO()
        with TerseDecompresser.create(io.BytesIO(header_bytes), out) as d:
            d.decode()
        assert out.getvalue() == b""

    def test_pack_vb_header_only_binary_produces_empty_output(self) -> None:
        """VB PACK binary mode with no payload produces empty output."""
        header_bytes = _build_pack_header(
            record_len1=80, record_len2=80, variable_flag=0x01
        )
        out = io.BytesIO()
        with TerseDecompresser.create(
            io.BytesIO(header_bytes), out, text_mode=False
        ) as d:
            d.decode()
        assert out.getvalue() == b""
