"""
Tests for SpooledTemporaryFile-based decompression in TerseFile (feat/spooled-streamdecompressor).

Covers:
- Small output stays in RAM (SpooledTemporaryFile._rolled is False)
- Large output spills to disk (SpooledTemporaryFile._rolled is True)
- spool_max_bytes=0 always uses disk
- spool_max_bytes=None uses BytesIO (legacy RAM path)
- read(), readinto(), seek(), tell() work for both RAM and disk paths
- close() removes the /tmp spill file
- Round-trip: output identical to core.decompress()
- tersedecompress.open() passes spool_max_bytes through correctly

All tests are self-contained — they synthesize valid tersed byte streams
using the same 12-bit PACK encoding as the block_reader, so no external
test-data files are required.
"""

import io
import struct
import tempfile
from pathlib import Path
from typing import Iterator

import pytest

import tersedecompress
from tersedecompress.core import decompress
from tersedecompress.file import TerseFile, _DEFAULT_SPOOL_MAX_BYTES


# ---------------------------------------------------------------------------
# Helpers — minimal in-memory PACK tersed stream builder
# ---------------------------------------------------------------------------


def _build_pack_header(record_len: int = 80, variable_flag: int = 0x00) -> bytes:
    """Build a 12-byte PACK host-mode header (version 0x02)."""
    h = bytearray()
    h.append(0x02)                        # version: PACK host mode
    h.append(variable_flag)
    h += struct.pack(">H", record_len)    # RecordLen1
    h.append(0x00)                        # flags
    h.append(0x00)                        # ratio
    h += struct.pack(">H", 0x0000)        # BlockSize
    h += struct.pack(">i", record_len)    # RecordLen2
    return bytes(h)                       # 12 bytes


def _encode_pack_payload(values: list[int]) -> bytes:
    """Encode a list of 12-bit integers into the PACK 3-bytes-per-2-values format.

    Two 12-bit values are packed into 3 bytes:
      byte1 = v1 >> 4
      byte2 = ((v1 & 0xF) << 4) | (v2 >> 8)
      byte3 = v2 & 0xFF
    When the list has an odd length, ENDOFFILE (0) is appended as the
    second value of the last pair.
    """
    result = bytearray()
    it = iter(values)
    for v1 in it:
        try:
            v2 = next(it)
        except StopIteration:
            v2 = 0  # pad with ENDOFFILE
        byte1 = (v1 >> 4) & 0xFF
        byte2 = ((v1 & 0x0F) << 4) | ((v2 >> 8) & 0x0F)
        byte3 = v2 & 0xFF
        result.extend([byte1, byte2, byte3])
    return bytes(result)


def make_tersed(content: bytes, record_len: int = 0) -> bytes:
    """Build a minimal PACK-encoded tersed byte string whose decompression yields *content*.

    Args:
        content:    The bytes that should appear in the decompressed output.
                    Must be a multiple of *record_len* (padding is the caller's
                    responsibility).  When *record_len* is 0, it defaults to
                    ``len(content)`` so that a single record is produced.
        record_len: Fixed-record length used in the TERSE header.  Defaults to
                    ``len(content)`` if 0.

    Returns:
        A bytes object that can be passed to ``TerseFile`` or ``decompress()``.
    """
    if record_len == 0:
        record_len = len(content) if content else 1
    header = _build_pack_header(record_len=record_len)
    # 1-based codepoints; ENDOFFILE (0) terminates the stream
    values = [b + 1 for b in content]
    values.append(0)  # ENDOFFILE
    payload = _encode_pack_payload(values)
    return header + payload


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def small_content() -> bytes:
    """80 bytes that fit comfortably below any reasonable spool threshold."""
    return b"A" * 80


@pytest.fixture()
def small_tersed(small_content: bytes) -> bytes:
    return make_tersed(small_content, record_len=80)


@pytest.fixture()
def large_content() -> bytes:
    """1 600 bytes — guaranteed to exceed a spool_max_bytes=512 threshold."""
    return b"X" * 80 * 20  # 20 records of 80 bytes = 1600 bytes


@pytest.fixture()
def large_tersed(large_content: bytes) -> bytes:
    return make_tersed(large_content, record_len=80)


# ---------------------------------------------------------------------------
# Round-trip: SpooledTemporaryFile output matches core.decompress()
# ---------------------------------------------------------------------------


class TestRoundTrip:
    def test_small_matches_core_decompress(
        self, small_tersed: bytes, small_content: bytes
    ) -> None:
        """SpooledTemporaryFile output is byte-identical to core.decompress()."""
        expected = decompress(small_tersed, text_mode=False)
        assert expected == small_content

        src = io.BytesIO(small_tersed)
        with TerseFile(src, spool_max_bytes=_DEFAULT_SPOOL_MAX_BYTES) as f:
            result = f.read()
        assert result == expected

    def test_large_matches_core_decompress(
        self, large_tersed: bytes, large_content: bytes
    ) -> None:
        """Disk-spilled output is byte-identical to core.decompress()."""
        expected = decompress(large_tersed, text_mode=False)
        assert expected == large_content

        src = io.BytesIO(large_tersed)
        with TerseFile(src, spool_max_bytes=256) as f:  # threshold below 1600
            result = f.read()
        assert result == expected


# ---------------------------------------------------------------------------
# RAM vs disk threshold (_rolled attribute of SpooledTemporaryFile)
# ---------------------------------------------------------------------------


class TestSpoolThreshold:
    def test_small_stays_in_ram(self, small_tersed: bytes) -> None:
        """Output smaller than spool_max_bytes must remain in RAM (_rolled=False)."""
        src = io.BytesIO(small_tersed)
        # spool_max_bytes much larger than 80-byte output → no roll to disk
        with TerseFile(src, spool_max_bytes=1024 * 1024) as f:
            f.read()
            assert f._buffer is not None
            assert isinstance(f._buffer, tempfile.SpooledTemporaryFile)
            assert not f._buffer._rolled  # type: ignore[union-attr]

    def test_large_spills_to_disk(self, large_tersed: bytes) -> None:
        """Output exceeding spool_max_bytes must be rolled to a disk file."""
        src = io.BytesIO(large_tersed)
        # spool_max_bytes = 256, output is 1600 bytes → must roll
        with TerseFile(src, spool_max_bytes=256) as f:
            f.read()
            assert f._buffer is not None
            assert isinstance(f._buffer, tempfile.SpooledTemporaryFile)
            assert f._buffer._rolled  # type: ignore[union-attr]

    def test_spool_max_bytes_zero_always_disk(self, small_tersed: bytes) -> None:
        """spool_max_bytes=0 must always roll to disk regardless of output size."""
        src = io.BytesIO(small_tersed)
        with TerseFile(src, spool_max_bytes=0) as f:
            f.read()
            assert f._buffer is not None
            assert isinstance(f._buffer, tempfile.SpooledTemporaryFile)
            assert f._buffer._rolled  # type: ignore[union-attr]

    def test_spool_max_bytes_none_uses_bytesio(self, small_tersed: bytes) -> None:
        """spool_max_bytes=None must use plain BytesIO (legacy RAM path)."""
        src = io.BytesIO(small_tersed)
        with TerseFile(src, spool_max_bytes=None) as f:
            f.read()
            assert isinstance(f._buffer, io.BytesIO)


# ---------------------------------------------------------------------------
# read() / readinto() / seek() / tell() — both RAM and disk paths
# ---------------------------------------------------------------------------


class TestIOOperationsRam:
    """Verify that standard IO operations work when output stays in RAM."""

    def test_read_full(self, small_tersed: bytes, small_content: bytes) -> None:
        with TerseFile(io.BytesIO(small_tersed), spool_max_bytes=1024 * 1024) as f:
            assert f.read() == small_content

    def test_read_chunked(self, small_tersed: bytes, small_content: bytes) -> None:
        chunks: list[bytes] = []
        with TerseFile(io.BytesIO(small_tersed), spool_max_bytes=1024 * 1024) as f:
            while chunk := f.read(16):
                chunks.append(chunk)
        assert b"".join(chunks) == small_content

    def test_readinto(self, small_tersed: bytes, small_content: bytes) -> None:
        buf = bytearray(len(small_content))
        with TerseFile(io.BytesIO(small_tersed), spool_max_bytes=1024 * 1024) as f:
            n = f.readinto(buf)
        assert n == len(small_content)
        assert bytes(buf) == small_content

    def test_seek_and_reread(self, small_tersed: bytes, small_content: bytes) -> None:
        with TerseFile(io.BytesIO(small_tersed), spool_max_bytes=1024 * 1024) as f:
            first = f.read()
            f.seek(0)
            second = f.read()
        assert first == second == small_content

    def test_tell(self, small_tersed: bytes, small_content: bytes) -> None:
        n = min(10, len(small_content))
        with TerseFile(io.BytesIO(small_tersed), spool_max_bytes=1024 * 1024) as f:
            assert f.tell() == 0
            f.read(n)
            assert f.tell() == n

    def test_seek_from_end(self, small_tersed: bytes, small_content: bytes) -> None:
        tail_len = min(5, len(small_content))
        with TerseFile(io.BytesIO(small_tersed), spool_max_bytes=1024 * 1024) as f:
            f.seek(-tail_len, 2)
            tail = f.read()
        assert tail == small_content[-tail_len:]


class TestIOOperationsDisk:
    """Same suite but with spool_max_bytes=0 (always disk)."""

    def test_read_full(self, large_tersed: bytes, large_content: bytes) -> None:
        with TerseFile(io.BytesIO(large_tersed), spool_max_bytes=0) as f:
            assert f.read() == large_content

    def test_read_chunked(self, large_tersed: bytes, large_content: bytes) -> None:
        chunks: list[bytes] = []
        with TerseFile(io.BytesIO(large_tersed), spool_max_bytes=0) as f:
            while chunk := f.read(256):
                chunks.append(chunk)
        assert b"".join(chunks) == large_content

    def test_readinto(self, large_tersed: bytes, large_content: bytes) -> None:
        buf = bytearray(len(large_content))
        with TerseFile(io.BytesIO(large_tersed), spool_max_bytes=0) as f:
            n = f.readinto(buf)
        assert n == len(large_content)
        assert bytes(buf) == large_content

    def test_seek_and_reread(self, large_tersed: bytes, large_content: bytes) -> None:
        with TerseFile(io.BytesIO(large_tersed), spool_max_bytes=0) as f:
            first = f.read()
            f.seek(0)
            second = f.read()
        assert first == second == large_content

    def test_tell(self, large_tersed: bytes, large_content: bytes) -> None:
        n = 100
        with TerseFile(io.BytesIO(large_tersed), spool_max_bytes=0) as f:
            assert f.tell() == 0
            f.read(n)
            assert f.tell() == n

    def test_seek_from_end(self, large_tersed: bytes, large_content: bytes) -> None:
        tail_len = 80
        with TerseFile(io.BytesIO(large_tersed), spool_max_bytes=0) as f:
            f.seek(-tail_len, 2)
            tail = f.read()
        assert tail == large_content[-tail_len:]


# ---------------------------------------------------------------------------
# close() cleans up the /tmp spill file
# ---------------------------------------------------------------------------


class TestCloseCleanup:
    def test_close_removes_tmp_file(self, large_tersed: bytes) -> None:
        """After close(), the disk-spill file descriptor must be closed.

        In Python 3.12+, SpooledTemporaryFile uses an anonymous temporary file
        (deleted from the filesystem immediately, accessible only via its file
        descriptor).  We therefore verify cleanup by checking that the FD
        becomes invalid after close(), rather than checking a filesystem path.
        """
        import os

        f = TerseFile(io.BytesIO(large_tersed), spool_max_bytes=0)
        f.read()  # trigger decompression + disk spill

        assert f._buffer is not None
        buf = f._buffer
        assert isinstance(buf, tempfile.SpooledTemporaryFile)
        assert buf._rolled  # type: ignore[union-attr]

        # Obtain the file descriptor of the underlying rolled file.
        underlying = buf._file  # type: ignore[union-attr]  # io.BufferedRandom in CPython 3.12
        fd = underlying.fileno()
        # Confirm the FD is open before close()
        assert os.fstat(fd) is not None

        f.close()

        assert f.closed
        assert f._buffer is None
        # After close(), the FD must be invalid
        with pytest.raises(OSError):
            os.fstat(fd)

    def test_context_manager_closes(self, small_tersed: bytes) -> None:
        """TerseFile is closed after exiting the with-block."""
        with TerseFile(io.BytesIO(small_tersed), spool_max_bytes=0) as f:
            f.read()
        assert f.closed

    def test_close_without_read_does_not_raise(self, small_tersed: bytes) -> None:
        """Closing before any read() is safe (buffer is None)."""
        f = TerseFile(io.BytesIO(small_tersed))
        f.close()
        assert f.closed


# ---------------------------------------------------------------------------
# tersedecompress.open() passes spool_max_bytes through
# ---------------------------------------------------------------------------


class TestOpenPassesSpool:
    def test_open_with_path_uses_spool(
        self, small_tersed: bytes, small_content: bytes, tmp_path: Path
    ) -> None:
        """tersedecompress.open(path, spool_max_bytes=0) spills to disk."""
        tersed_file = tmp_path / "test.tersed"
        tersed_file.write_bytes(small_tersed)

        with tersedecompress.open(tersed_file, spool_max_bytes=0) as f:
            result = f.read()
            assert isinstance(f._buffer, tempfile.SpooledTemporaryFile)
            assert f._buffer._rolled  # type: ignore[union-attr]
        assert result == small_content

    def test_open_with_stream_uses_spool(
        self, small_tersed: bytes, small_content: bytes
    ) -> None:
        """tersedecompress.open(stream, spool_max_bytes=0) spills to disk."""
        with tersedecompress.open(io.BytesIO(small_tersed), spool_max_bytes=0) as f:
            result = f.read()
            assert isinstance(f._buffer, tempfile.SpooledTemporaryFile)
        assert result == small_content

    def test_open_default_spool_max_bytes(self, small_tersed: bytes) -> None:
        """Default spool_max_bytes matches _DEFAULT_SPOOL_MAX_BYTES (8 MB)."""
        assert _DEFAULT_SPOOL_MAX_BYTES == 8 * 1024 * 1024
        with tersedecompress.open(io.BytesIO(small_tersed)) as f:
            assert f._spool_max_bytes == _DEFAULT_SPOOL_MAX_BYTES

    def test_open_spool_max_bytes_none_legacy(self, small_tersed: bytes) -> None:
        """spool_max_bytes=None gives BytesIO (legacy behaviour)."""
        with tersedecompress.open(io.BytesIO(small_tersed), spool_max_bytes=None) as f:
            f.read()
            assert isinstance(f._buffer, io.BytesIO)


# ---------------------------------------------------------------------------
# Default constant value
# ---------------------------------------------------------------------------


def test_default_spool_max_bytes_is_8mb() -> None:
    """The module default must be exactly 8 MiB."""
    assert _DEFAULT_SPOOL_MAX_BYTES == 8 * 1024 * 1024
