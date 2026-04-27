"""
Tests for TerseStreamFile — forward-only, no-seek, no-temp-file streaming
decompression (feat/streaming-forward-only-readio).

Covers:
- Round-trip: identical output to decompress() / TerseFile
- seekable() returns False
- seek() raises io.UnsupportedOperation
- tell() raises io.UnsupportedOperation
- Backpressure: small chunk_buffer_count, slow consumer
- Error propagation: invalid source bytes → read() raises
- close() before EOF (producer-thread clean shutdown)
- Pipe pattern: shutil.copyfileobj produces identical output
- Iteration: iter(lambda: f.read(4096), b"") pattern
- tersedecompress.open(..., streaming=True) API
- Memory profile via tracemalloc (optional, logged but not asserted hard)

Tests are split into two groups:
- *_synthetic: use a minimal header-only PACK stream (no external data needed)
- the rest: use real test data and skip gracefully if absent
"""

import io
import shutil
import struct
import threading
import time
import tracemalloc
from pathlib import Path

import pytest

import tersedecompress
from tersedecompress import decompress
from tersedecompress.file import TerseStreamFile


# ---------------------------------------------------------------------------
# Helpers — build a minimal valid TERSE PACK stream without real test data
# ---------------------------------------------------------------------------


def _make_empty_pack_stream() -> bytes:
    """Return a TERSE PACK stream that decompresses to b"" (header only, no payload).

    Uses the same header layout as test_edge_cases.py::_build_pack_header.
    The block reader immediately returns ENDOFFILE when there is no payload,
    so the decompressor emits zero output bytes without errors.
    """
    header = bytearray()
    header.append(0x02)                          # version: PACK host mode
    header.append(0x00)                          # variable_flag: FB
    header += struct.pack(">H", 80)              # RecordLen1
    header.append(0x00)                          # flags
    header.append(0x00)                          # ratio
    header += struct.pack(">H", 0)               # BlockSize
    header += struct.pack(">i", 80)              # RecordLen2
    return bytes(header)


_EMPTY_PACK_BYTES: bytes = _make_empty_pack_stream()


# ---------------------------------------------------------------------------
# Fixtures: require real test data; skip gracefully when absent
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
    """Expected binary decompression output."""
    return decompress(pack_bytes, text_mode=False)


@pytest.fixture(scope="session")
def expected_text(pack_bytes: bytes) -> bytes:
    """Expected text-mode decompression output."""
    return decompress(pack_bytes, text_mode=True)


@pytest.fixture(scope="session")
def large_pack_file(test_data_dir: Path) -> Path:
    """Return path to a larger PACK file for the memory-profile test."""
    p = test_data_dir / "TERSED" / "FB.BIBLE.TXT.PACK"
    if not p.exists():
        pytest.skip(f"Test file not found: {p}")
    return p


# ---------------------------------------------------------------------------
# 1. seekable / seek / tell contract (synthetic — no real test data needed)
# ---------------------------------------------------------------------------


class TestNoSeekContractSynthetic:
    """API contract tests using a minimal header-only stream."""

    def test_seekable_returns_false(self) -> None:
        """TerseStreamFile.seekable() must return False."""
        with TerseStreamFile(io.BytesIO(_EMPTY_PACK_BYTES)) as f:
            assert f.seekable() is False

    def test_seek_raises_unsupported(self) -> None:
        """seek() must raise io.UnsupportedOperation."""
        with TerseStreamFile(io.BytesIO(_EMPTY_PACK_BYTES)) as f:
            with pytest.raises(io.UnsupportedOperation):
                f.seek(0)

    def test_tell_raises_unsupported(self) -> None:
        """tell() must raise io.UnsupportedOperation."""
        with TerseStreamFile(io.BytesIO(_EMPTY_PACK_BYTES)) as f:
            with pytest.raises(io.UnsupportedOperation):
                f.tell()

    def test_readable_returns_true(self) -> None:
        """readable() must return True."""
        with TerseStreamFile(io.BytesIO(_EMPTY_PACK_BYTES)) as f:
            assert f.readable() is True

    def test_read_empty_pack_produces_empty_bytes(self) -> None:
        """A header-only PACK stream decompresses to b""."""
        with TerseStreamFile(io.BytesIO(_EMPTY_PACK_BYTES)) as f:
            result = f.read()
        assert result == b""

    def test_read_zero_returns_empty(self) -> None:
        """read(0) always returns b"" without blocking."""
        with TerseStreamFile(io.BytesIO(_EMPTY_PACK_BYTES)) as f:
            assert f.read(0) == b""

    def test_isinstance_raw_io_base(self) -> None:
        """TerseStreamFile must be a subclass of io.RawIOBase."""
        with TerseStreamFile(io.BytesIO(_EMPTY_PACK_BYTES)) as f:
            assert isinstance(f, io.RawIOBase)


# ---------------------------------------------------------------------------
# 2. close() behaviour (synthetic — no real test data needed)
# ---------------------------------------------------------------------------


class TestCloseSynthetic:
    def test_context_manager_closes(self) -> None:
        """File is closed after exiting the with-block."""
        with TerseStreamFile(io.BytesIO(_EMPTY_PACK_BYTES)) as f:
            f.read()
        assert f.closed

    def test_close_without_read_does_not_hang(self) -> None:
        """close() immediately after construction must exit cleanly."""
        f = TerseStreamFile(io.BytesIO(_EMPTY_PACK_BYTES))
        f.close()
        assert f.closed

    def test_read_after_close_raises_value_error(self) -> None:
        """read() on a closed TerseStreamFile raises ValueError."""
        f = TerseStreamFile(io.BytesIO(_EMPTY_PACK_BYTES))
        f.close()
        with pytest.raises(ValueError, match="I/O operation on closed file"):
            f.read()

    def test_double_close_is_idempotent(self) -> None:
        """Calling close() twice must not raise."""
        f = TerseStreamFile(io.BytesIO(_EMPTY_PACK_BYTES))
        f.close()
        f.close()  # must not raise
        assert f.closed


# ---------------------------------------------------------------------------
# 3. Error propagation (synthetic)
# ---------------------------------------------------------------------------


class TestErrorPropagationSynthetic:
    def test_invalid_source_raises_on_read(self) -> None:
        """Invalid TERSE bytes propagate as IOError to the calling thread."""
        bad_data = b"\xFF" * 64  # not a valid TERSE header
        with TerseStreamFile(io.BytesIO(bad_data)) as f:
            with pytest.raises((IOError, OSError)):
                f.read()

    def test_empty_source_raises_on_read(self) -> None:
        """Completely empty source raises IOError (missing header)."""
        with TerseStreamFile(io.BytesIO(b"")) as f:
            with pytest.raises((IOError, OSError)):
                f.read()


# ---------------------------------------------------------------------------
# 4. Round-trip correctness (requires real test data)
# ---------------------------------------------------------------------------


class TestRoundTrip:
    def test_read_all_equals_decompress(
        self, pack_bytes: bytes, expected_binary: bytes
    ) -> None:
        """read() with no argument produces identical output to decompress()."""
        with TerseStreamFile(io.BytesIO(pack_bytes)) as f:
            result = f.read()
        assert result == expected_binary

    def test_read_minus_one_equals_decompress(
        self, pack_bytes: bytes, expected_binary: bytes
    ) -> None:
        """read(-1) is equivalent to read()."""
        with TerseStreamFile(io.BytesIO(pack_bytes)) as f:
            result = f.read(-1)
        assert result == expected_binary

    def test_chunked_4096_equals_decompress(
        self, pack_bytes: bytes, expected_binary: bytes
    ) -> None:
        """Concatenated read(4096) chunks equal a single read()."""
        chunks: list[bytes] = []
        with TerseStreamFile(io.BytesIO(pack_bytes)) as f:
            while chunk := f.read(4096):
                chunks.append(chunk)
        assert b"".join(chunks) == expected_binary

    def test_chunked_1_equals_decompress(
        self, pack_bytes: bytes, expected_binary: bytes
    ) -> None:
        """Single-byte reads produce correct output (stress leftover handling)."""
        parts: list[bytes] = []
        with TerseStreamFile(io.BytesIO(pack_bytes)) as f:
            while (b := f.read(1)):
                parts.append(b)
        assert b"".join(parts) == expected_binary

    def test_text_mode_equals_decompress(
        self, pack_bytes: bytes, expected_text: bytes
    ) -> None:
        """text_mode=True produces the same output as decompress(text_mode=True)."""
        with TerseStreamFile(io.BytesIO(pack_bytes), text_mode=True) as f:
            result = f.read()
        assert result == expected_text

    def test_eof_returns_empty_bytes(self, pack_bytes: bytes) -> None:
        """A second read() after exhaustion returns b""."""
        with TerseStreamFile(io.BytesIO(pack_bytes)) as f:
            f.read()
            assert f.read() == b""
            assert f.read(100) == b""


# ---------------------------------------------------------------------------
# 5. readinto()
# ---------------------------------------------------------------------------


class TestReadInto:
    def test_readinto_full_buffer(
        self, pack_bytes: bytes, expected_binary: bytes
    ) -> None:
        """readinto() with a full-size buffer fills it completely."""
        buf = bytearray(len(expected_binary))
        with TerseStreamFile(io.BytesIO(pack_bytes)) as f:
            n = f.readinto(buf)
        assert n == len(expected_binary)
        assert bytes(buf) == expected_binary

    def test_readinto_small_buffer(
        self, pack_bytes: bytes, expected_binary: bytes
    ) -> None:
        """readinto() with a small buffer returns at most len(buffer) bytes."""
        chunk = min(16, len(expected_binary))
        buf = bytearray(chunk)
        with TerseStreamFile(io.BytesIO(pack_bytes)) as f:
            n = f.readinto(buf)
        assert n == chunk
        assert bytes(buf) == expected_binary[:chunk]


# ---------------------------------------------------------------------------
# 6. Pipe pattern — shutil.copyfileobj
# ---------------------------------------------------------------------------


class TestPipePattern:
    def test_copyfileobj_equals_decompress(
        self, pack_bytes: bytes, expected_binary: bytes
    ) -> None:
        """shutil.copyfileobj(TerseStreamFile, BytesIO) produces identical output."""
        out = io.BytesIO()
        with TerseStreamFile(io.BytesIO(pack_bytes)) as f:
            shutil.copyfileobj(f, out)
        assert out.getvalue() == expected_binary

    def test_iteration_pattern_equals_decompress(
        self, pack_bytes: bytes, expected_binary: bytes
    ) -> None:
        """iter(lambda: f.read(4096), b"") pattern produces identical output."""
        parts: list[bytes] = []
        with TerseStreamFile(io.BytesIO(pack_bytes)) as f:
            for chunk in iter(lambda: f.read(4096), b""):
                parts.append(chunk)
        assert b"".join(parts) == expected_binary


# ---------------------------------------------------------------------------
# 7. tersedecompress.open(..., streaming=True) public API
# ---------------------------------------------------------------------------


class TestPublicOpenAPI:
    def test_open_path_streaming_returns_terse_stream_file(
        self, small_pack_file: Path
    ) -> None:
        """tersedecompress.open(path, streaming=True) returns TerseStreamFile."""
        with tersedecompress.open(small_pack_file, streaming=True) as f:
            assert isinstance(f, TerseStreamFile)

    def test_open_path_streaming_correct_output(
        self, small_pack_file: Path, expected_binary: bytes
    ) -> None:
        """tersedecompress.open(path, streaming=True) produces correct output."""
        with tersedecompress.open(small_pack_file, streaming=True) as f:
            result = f.read()
        assert result == expected_binary

    def test_open_stream_streaming_correct_output(
        self, small_pack_file: Path, expected_binary: bytes
    ) -> None:
        """tersedecompress.open(stream, streaming=True) reads from BinaryIO."""
        with small_pack_file.open("rb") as raw:
            with tersedecompress.open(raw, streaming=True) as f:
                result = f.read()
        assert result == expected_binary

    def test_open_streaming_false_is_default_seekable(
        self, small_pack_file: Path
    ) -> None:
        """streaming=False (default) still returns a seekable TerseFile."""
        with tersedecompress.open(small_pack_file) as f:
            assert f.seekable() is True
            assert isinstance(f, tersedecompress.TerseFile)

    def test_open_text_mode_streaming(
        self, small_pack_file: Path, expected_text: bytes
    ) -> None:
        """streaming=True combined with text_mode=True produces correct text output."""
        with tersedecompress.open(small_pack_file, text_mode=True, streaming=True) as f:
            result = f.read()
        assert result == expected_text

    def test_open_path_closes_source_after_read(
        self, small_pack_file: Path
    ) -> None:
        """When opened via path, the source stream is closed after context exit."""
        with tersedecompress.open(small_pack_file, streaming=True) as f:
            f.read()
        assert f.closed

    def test_open_stream_does_not_close_caller_stream(
        self, small_pack_file: Path
    ) -> None:
        """When caller passes a stream, TerseStreamFile must not close it."""
        raw = small_pack_file.open("rb")
        with tersedecompress.open(raw, streaming=True) as f:
            f.read()
        assert not raw.closed
        raw.close()


# ---------------------------------------------------------------------------
# 8. Error propagation with real data
# ---------------------------------------------------------------------------


class TestErrorPropagation:
    def test_max_output_bytes_exceeded_raises(
        self, pack_bytes: bytes, expected_binary: bytes
    ) -> None:
        """Exceeding max_output_bytes raises IOError during read()."""
        limit = max(1, len(expected_binary) // 2)
        with TerseStreamFile(io.BytesIO(pack_bytes), max_output_bytes=limit) as f:
            with pytest.raises((IOError, OSError)):
                f.read()

    def test_read_after_close_raises_value_error(
        self, pack_bytes: bytes
    ) -> None:
        """read() on a closed TerseStreamFile raises ValueError."""
        f = TerseStreamFile(io.BytesIO(pack_bytes))
        f.close()
        with pytest.raises(ValueError, match="I/O operation on closed file"):
            f.read()


# ---------------------------------------------------------------------------
# 9. close() before EOF — producer thread clean shutdown
# ---------------------------------------------------------------------------


class TestEarlyClose:
    def test_close_before_eof_does_not_hang(self, pack_bytes: bytes) -> None:
        """close() before reading all data must not block indefinitely."""
        f = TerseStreamFile(io.BytesIO(pack_bytes))
        f.read(4)   # read just a few bytes so the producer is alive
        deadline = time.monotonic() + 5.0
        f.close()
        assert time.monotonic() < deadline, "close() took too long"
        assert f.closed

    def test_close_without_any_read_does_not_hang(self, pack_bytes: bytes) -> None:
        """close() immediately after construction must exit cleanly."""
        f = TerseStreamFile(io.BytesIO(pack_bytes), chunk_buffer_count=2)
        f.close()
        assert f.closed

    def test_context_manager_closes_after_partial_read(
        self, pack_bytes: bytes
    ) -> None:
        """Partial read inside with-block: file is closed on __exit__."""
        with TerseStreamFile(io.BytesIO(pack_bytes)) as f:
            f.read(8)
        assert f.closed


# ---------------------------------------------------------------------------
# 10. Backpressure: tiny queue, slow consumer
# ---------------------------------------------------------------------------


class TestBackpressure:
    def test_small_queue_correct_output(
        self, pack_bytes: bytes, expected_binary: bytes
    ) -> None:
        """chunk_buffer_count=2 still produces correct output (tests backpressure)."""
        with TerseStreamFile(io.BytesIO(pack_bytes), chunk_buffer_count=2) as f:
            result = f.read()
        assert result == expected_binary

    def test_slow_consumer_does_not_lose_data(
        self, pack_bytes: bytes, expected_binary: bytes
    ) -> None:
        """Artificial delays in the consumer do not cause data loss or errors."""
        parts: list[bytes] = []
        with TerseStreamFile(io.BytesIO(pack_bytes), chunk_buffer_count=2) as f:
            while chunk := f.read(32):
                parts.append(chunk)
                time.sleep(0.0005)
        assert b"".join(parts) == expected_binary

    def test_producer_thread_exits_after_full_read(
        self, pack_bytes: bytes
    ) -> None:
        """Producer thread must have finished after all data is consumed."""
        with TerseStreamFile(io.BytesIO(pack_bytes), chunk_buffer_count=4) as f:
            f.read()
            f._thread.join(timeout=2.0)
            assert not f._thread.is_alive(), "Producer thread did not exit after EOF"


# ---------------------------------------------------------------------------
# 11. Thread-safety: concurrent reads from distinct instances
# ---------------------------------------------------------------------------


class TestThreadSafety:
    def test_two_instances_concurrently(
        self, pack_bytes: bytes, expected_binary: bytes
    ) -> None:
        """Two TerseStreamFile instances can decompress concurrently without
        interference."""
        results: list[bytes] = [b"", b""]
        errors: list[BaseException] = []

        def _read(idx: int) -> None:
            try:
                with TerseStreamFile(io.BytesIO(pack_bytes)) as f:
                    results[idx] = f.read()
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        t0 = threading.Thread(target=_read, args=(0,))
        t1 = threading.Thread(target=_read, args=(1,))
        t0.start()
        t1.start()
        t0.join(timeout=10)
        t1.join(timeout=10)

        assert not errors, f"Concurrent reads raised: {errors}"
        assert results[0] == expected_binary
        assert results[1] == expected_binary


# ---------------------------------------------------------------------------
# 12. Memory profile (optional — uses tracemalloc)
# ---------------------------------------------------------------------------


class TestMemoryProfile:
    def test_peak_memory_with_large_file(self, large_pack_file: Path) -> None:
        """Peak RAM during streaming should be far below the full output size.

        Uses tracemalloc.get_traced_memory()[1] (the true peak since tracing
        started) rather than a snapshot-diff, which only captures allocations
        that are still alive at snapshot time.
        """
        full_output = decompress(large_pack_file.read_bytes(), text_mode=False)

        tracemalloc.start()

        out = io.BytesIO()
        with tersedecompress.open(large_pack_file, streaming=True) as f:
            shutil.copyfileobj(f, out)

        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        assert out.getvalue() == full_output

        output_size = len(full_output)
        if output_size > 0:
            assert peak_bytes < output_size * 2, (
                f"Peak allocation {peak_bytes:,} bytes >= 2× output size "
                f"{output_size:,} bytes — possible non-streaming fallback"
            )

    def test_small_input_low_peak_ram(
        self, pack_bytes: bytes, expected_binary: bytes
    ) -> None:
        """Streaming a small file stays well under 1 MB peak allocation.

        Uses tracemalloc.get_traced_memory()[1] for true peak measurement.
        """
        tracemalloc.start()

        out = io.BytesIO()
        with TerseStreamFile(io.BytesIO(pack_bytes)) as f:
            shutil.copyfileobj(f, out)

        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        assert out.getvalue() == expected_binary
        # 1 MB is a very generous ceiling for a small test file.
        assert peak_bytes < 1 * 1024 * 1024, (
            f"Peak allocation {peak_bytes:,} bytes exceeded 1 MB for small input"
        )
