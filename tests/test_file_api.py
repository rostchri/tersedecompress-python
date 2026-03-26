"""
Tests for the TerseFile / tersedecompress.open() file-like API (Issue #18).

Covers:
- tersedecompress.open(path) with file path
- tersedecompress.open(stream) with an open BinaryIO
- read() without argument (full read)
- read(n) chunked reading
- seek(0) + re-read
- tell() position tracking
- context manager (__enter__/__exit__)
- text_mode=True
- CLI pipe mode via subprocess
"""

import subprocess
import sys
from pathlib import Path

import pytest

import tersedecompress
from tersedecompress.core import decompress


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def small_pack_file(test_data_dir: Path) -> Path:
    """Return path to FB.AAA.TXT.PACK — the smallest PACK test file."""
    p = test_data_dir / "TERSED" / "FB.AAA.TXT.PACK"
    if not p.exists():
        pytest.skip(f"Test file not found: {p}")
    return p


@pytest.fixture(scope="session")
def small_pack_binary_expected(small_pack_file: Path) -> bytes:
    """Expected binary decompression output for FB.AAA.TXT.PACK."""
    return decompress(small_pack_file.read_bytes(), text_mode=False)


@pytest.fixture(scope="session")
def small_pack_text_expected(small_pack_file: Path) -> bytes:
    """Expected text-mode decompression output for FB.AAA.TXT.PACK."""
    return decompress(small_pack_file.read_bytes(), text_mode=True)


# ---------------------------------------------------------------------------
# 1. open(path) — file path as str/Path
# ---------------------------------------------------------------------------


class TestOpenWithPath:
    def test_open_path_object(
        self, small_pack_file: Path, small_pack_binary_expected: bytes
    ) -> None:
        """tersedecompress.open(Path) returns correct decompressed content."""
        with tersedecompress.open(small_pack_file) as f:
            result = f.read()
        assert result == small_pack_binary_expected

    def test_open_str_path(
        self, small_pack_file: Path, small_pack_binary_expected: bytes
    ) -> None:
        """tersedecompress.open(str) works the same as open(Path)."""
        with tersedecompress.open(str(small_pack_file)) as f:
            result = f.read()
        assert result == small_pack_binary_expected

    def test_open_closes_file_after_context(self, small_pack_file: Path) -> None:
        """File handle opened internally must be closed after the with-block."""
        with tersedecompress.open(small_pack_file) as f:
            pass
        assert f.closed


# ---------------------------------------------------------------------------
# 2. open(stream) — caller-provided stream
# ---------------------------------------------------------------------------


class TestOpenWithStream:
    def test_open_stream(
        self, small_pack_file: Path, small_pack_binary_expected: bytes
    ) -> None:
        """tersedecompress.open(stream) decompresses from an open BinaryIO."""
        with small_pack_file.open("rb") as raw:
            with tersedecompress.open(raw) as f:
                result = f.read()
        assert result == small_pack_binary_expected

    def test_open_stream_does_not_close_caller_stream(
        self, small_pack_file: Path
    ) -> None:
        """When the caller owns the stream, TerseFile must not close it."""
        raw = small_pack_file.open("rb")
        with tersedecompress.open(raw) as f:
            f.read()
        assert not raw.closed
        raw.close()


# ---------------------------------------------------------------------------
# 3. read() — full read
# ---------------------------------------------------------------------------


class TestReadFull:
    def test_read_all(
        self, small_pack_file: Path, small_pack_binary_expected: bytes
    ) -> None:
        """read() without argument returns all decompressed bytes."""
        with tersedecompress.open(small_pack_file) as f:
            assert f.read() == small_pack_binary_expected

    def test_read_minus_one(
        self, small_pack_file: Path, small_pack_binary_expected: bytes
    ) -> None:
        """read(-1) is equivalent to read()."""
        with tersedecompress.open(small_pack_file) as f:
            assert f.read(-1) == small_pack_binary_expected


# ---------------------------------------------------------------------------
# 4. read(n) — chunked reading
# ---------------------------------------------------------------------------


class TestReadChunked:
    def test_chunked_read_equals_full_read(
        self, small_pack_file: Path, small_pack_binary_expected: bytes
    ) -> None:
        """Concatenated read(4096) chunks equal a single read()."""
        chunks: list[bytes] = []
        with tersedecompress.open(small_pack_file) as f:
            while chunk := f.read(4096):
                chunks.append(chunk)
        assert b"".join(chunks) == small_pack_binary_expected

    def test_chunked_read_total_length(
        self, small_pack_file: Path, small_pack_binary_expected: bytes
    ) -> None:
        """Total bytes from chunked reads match expected size."""
        total = 0
        with tersedecompress.open(small_pack_file) as f:
            while chunk := f.read(1024):
                total += len(chunk)
        assert total == len(small_pack_binary_expected)


# ---------------------------------------------------------------------------
# 5. seek(0) + re-read
# ---------------------------------------------------------------------------


class TestSeek:
    def test_seek_zero_and_reread(
        self, small_pack_file: Path, small_pack_binary_expected: bytes
    ) -> None:
        """seek(0) after read() allows re-reading identical content."""
        with tersedecompress.open(small_pack_file) as f:
            first = f.read()
            f.seek(0)
            second = f.read()
        assert first == second == small_pack_binary_expected

    def test_seek_to_offset(
        self, small_pack_file: Path, small_pack_binary_expected: bytes
    ) -> None:
        """seek(n) positions the stream at byte n."""
        offset = min(10, len(small_pack_binary_expected))
        with tersedecompress.open(small_pack_file) as f:
            f.seek(offset)
            tail = f.read()
        assert tail == small_pack_binary_expected[offset:]

    def test_seek_from_end(
        self, small_pack_file: Path, small_pack_binary_expected: bytes
    ) -> None:
        """seek(-n, 2) positions n bytes before end."""
        n = min(5, len(small_pack_binary_expected))
        with tersedecompress.open(small_pack_file) as f:
            f.seek(-n, 2)
            tail = f.read()
        assert tail == small_pack_binary_expected[-n:]


# ---------------------------------------------------------------------------
# 6. tell() — position tracking
# ---------------------------------------------------------------------------


class TestTell:
    def test_tell_at_start(self, small_pack_file: Path) -> None:
        """tell() returns 0 before any read."""
        with tersedecompress.open(small_pack_file) as f:
            assert f.tell() == 0

    def test_tell_after_partial_read(
        self, small_pack_file: Path, small_pack_binary_expected: bytes
    ) -> None:
        """tell() returns correct position after read(n)."""
        n = min(10, len(small_pack_binary_expected))
        with tersedecompress.open(small_pack_file) as f:
            f.read(n)
            assert f.tell() == n

    def test_tell_after_seek(self, small_pack_file: Path) -> None:
        """tell() reflects position set by seek()."""
        with tersedecompress.open(small_pack_file) as f:
            f.seek(7)
            assert f.tell() == 7


# ---------------------------------------------------------------------------
# 7. Context manager
# ---------------------------------------------------------------------------


class TestContextManager:
    def test_context_manager_closes(self, small_pack_file: Path) -> None:
        """TerseFile is closed after exiting the with-block."""
        with tersedecompress.open(small_pack_file) as f:
            f.read()
        assert f.closed

    def test_context_manager_returns_terse_file(self, small_pack_file: Path) -> None:
        """The context manager yields a TerseFile instance."""
        with tersedecompress.open(small_pack_file) as f:
            assert isinstance(f, tersedecompress.TerseFile)


# ---------------------------------------------------------------------------
# 8. text_mode=True
# ---------------------------------------------------------------------------


class TestTextMode:
    def test_text_mode_result_differs_from_binary(
        self,
        small_pack_file: Path,
        small_pack_binary_expected: bytes,
        small_pack_text_expected: bytes,
    ) -> None:
        """text_mode=True yields different output than binary mode."""
        with tersedecompress.open(small_pack_file, text_mode=True) as f:
            text_result = f.read()
        assert text_result == small_pack_text_expected
        assert text_result != small_pack_binary_expected

    def test_text_mode_matches_decompress(
        self, small_pack_file: Path, small_pack_text_expected: bytes
    ) -> None:
        """tersedecompress.open(path, text_mode=True) matches decompress()."""
        with tersedecompress.open(small_pack_file, text_mode=True) as f:
            result = f.read()
        assert result == small_pack_text_expected


# ---------------------------------------------------------------------------
# 9. CLI pipe mode
# ---------------------------------------------------------------------------


class TestCliPipeMode:
    def _run(self, args: list[str], stdin_data: bytes | None = None) -> subprocess.CompletedProcess[bytes]:  # noqa: E501
        return subprocess.run(
            [sys.executable, "-m", "tersedecompress"] + args,
            input=stdin_data,
            capture_output=True,
        )

    def test_pipe_flag_stdin_to_stdout(
        self, small_pack_file: Path, small_pack_binary_expected: bytes
    ) -> None:
        """--pipe reads stdin and writes to stdout in binary mode."""
        stdin_data = small_pack_file.read_bytes()
        result = self._run(["--pipe", "-b"], stdin_data=stdin_data)
        assert result.returncode == 0
        assert result.stdout == small_pack_binary_expected

    def test_pipe_dash_input_to_stdout(
        self, small_pack_file: Path, small_pack_binary_expected: bytes
    ) -> None:
        """'- -' reads stdin and writes to stdout."""
        stdin_data = small_pack_file.read_bytes()
        result = self._run(["-", "-", "-b"], stdin_data=stdin_data)
        assert result.returncode == 0
        assert result.stdout == small_pack_binary_expected

    def test_input_dash_output_file(
        self, small_pack_file: Path, small_pack_binary_expected: bytes, tmp_path: Path
    ) -> None:
        """'-' as input reads from stdin; explicit output path writes file."""
        out_file = tmp_path / "out.bin"
        stdin_data = small_pack_file.read_bytes()
        result = self._run(["-", str(out_file), "-b"], stdin_data=stdin_data)
        assert result.returncode == 0
        assert out_file.read_bytes() == small_pack_binary_expected

    def test_input_file_dash_output(
        self, small_pack_file: Path, small_pack_binary_expected: bytes
    ) -> None:
        """'-' as output writes to stdout; explicit input file is read."""
        result = self._run([str(small_pack_file), "-", "-b"])
        assert result.returncode == 0
        assert result.stdout == small_pack_binary_expected

    def test_pipe_no_info_logs_on_stdout(
        self, small_pack_file: Path
    ) -> None:
        """In pipe/stdout mode, stdout must contain ONLY decompressed bytes."""
        stdin_data = small_pack_file.read_bytes()
        result = self._run(["--pipe", "-b"], stdin_data=stdin_data)
        # stdout must start with binary data, not a log line
        assert not result.stdout.startswith(b"INFO")


# ---------------------------------------------------------------------------
# 10. Error-path tests (Issue #22)
# ---------------------------------------------------------------------------


class TestErrorPaths:
    def test_read_after_close_raises_value_error(self, small_pack_file: Path) -> None:
        """read() on a closed TerseFile must raise ValueError."""
        f = tersedecompress.open(small_pack_file)
        f.close()
        with pytest.raises(ValueError, match="I/O operation on closed file"):
            f.read()

    def test_seek_after_close_raises_value_error(self, small_pack_file: Path) -> None:
        """seek() on a closed TerseFile must raise ValueError."""
        f = tersedecompress.open(small_pack_file)
        f.close()
        with pytest.raises(ValueError, match="I/O operation on closed file"):
            f.seek(0)

    def test_tell_after_close_raises_value_error(self, small_pack_file: Path) -> None:
        """tell() on a closed TerseFile must raise ValueError."""
        f = tersedecompress.open(small_pack_file)
        f.close()
        with pytest.raises(ValueError, match="I/O operation on closed file"):
            f.tell()

    def test_max_output_bytes_exceeded_raises_ioerror(
        self, small_pack_file: Path, small_pack_binary_expected: bytes
    ) -> None:
        """Exceeding max_output_bytes must raise IOError."""
        limit = max(1, len(small_pack_binary_expected) // 2)
        with pytest.raises(IOError):
            with tersedecompress.open(small_pack_file, max_output_bytes=limit) as f:
                f.read()

    def test_readinto_with_bytearray(
        self, small_pack_file: Path, small_pack_binary_expected: bytes
    ) -> None:
        """readinto() fills a bytearray and returns the number of bytes read."""
        buf = bytearray(len(small_pack_binary_expected))
        with tersedecompress.open(small_pack_file) as f:
            n = f.readinto(buf)
        assert n == len(small_pack_binary_expected)
        assert bytes(buf[:n]) == small_pack_binary_expected

    def test_readinto_partial_buffer(
        self, small_pack_file: Path, small_pack_binary_expected: bytes
    ) -> None:
        """readinto() with a smaller buffer reads only as many bytes as the buffer holds."""
        chunk_size = min(16, len(small_pack_binary_expected))
        buf = bytearray(chunk_size)
        with tersedecompress.open(small_pack_file) as f:
            n = f.readinto(buf)
        assert n == chunk_size
        assert bytes(buf) == small_pack_binary_expected[:chunk_size]
