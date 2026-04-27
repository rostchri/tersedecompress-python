"""File-like interface for reading TERSE-compressed data."""

import io
import tempfile
from pathlib import Path
from typing import BinaryIO

from .base import TerseDecompresser

_DEFAULT_SPOOL_MAX_BYTES: int = 8 * 1024 * 1024  # 8 MB


class TerseFile(io.RawIOBase):
    """Read-only file-like wrapper that decompresses TERSE data on access.

    Decompresses the input stream lazily on first read, writing the output
    into a SpooledTemporaryFile.  Small outputs (< spool_max_bytes) are kept
    in RAM; larger outputs are transparently spilled to a temporary file in
    /tmp, which is deleted automatically on close().

    Supports read(), readinto(), seek(), tell(), and context manager.

    Usage:
        with TerseFile(open("data.tersed", "rb")) as f:
            content = f.read()
    """

    def __init__(
        self,
        source: BinaryIO,
        text_mode: bool = False,
        max_output_bytes: int | None = None,
        spool_max_bytes: int | None = _DEFAULT_SPOOL_MAX_BYTES,
        *,
        _close_source: bool = False,
    ) -> None:
        """Initialise TerseFile.

        Args:
            source:           An open binary stream containing TERSE-compressed
                              data.
            text_mode:        If True, perform EBCDIC→ASCII conversion during
                              decompression.
            max_output_bytes: Upper bound on decompressed size.  None means
                              unlimited.
            spool_max_bytes:  Threshold in bytes below which decompressed output
                              is kept in RAM.  Above this threshold the output is
                              spilled to a temporary file in /tmp.
                              Pass 0 to always use a disk file.
                              Pass None to keep everything in RAM (legacy
                              behaviour, equivalent to old BytesIO path).
                              Default: 8 MB.
            _close_source:    Internal flag — set to True when TerseFile owns
                              the stream and must close it on close().
        """
        super().__init__()
        self._source: BinaryIO = source
        self._text_mode: bool = text_mode
        self._max_output_bytes: int | None = max_output_bytes
        self._spool_max_bytes: int | None = spool_max_bytes
        self._close_source: bool = _close_source
        # Populated lazily on first access.
        self._buffer: tempfile.SpooledTemporaryFile[bytes] | io.BytesIO | None = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_decompressed(self) -> None:
        """Decompress source into the internal buffer if not done yet.

        The source stream is passed directly as in_stream to
        TerseDecompresser.create() — no full read() into RAM.  Output goes
        into a SpooledTemporaryFile (or BytesIO when spool_max_bytes is None)
        and is seeked back to position 0 when done.

        Raises ValueError if the file is already closed.
        """
        if self.closed:
            raise ValueError("I/O operation on closed file")
        if self._buffer is not None:
            return

        if self._spool_max_bytes is None:
            # Legacy / unlimited-RAM opt-in: use plain BytesIO.
            out: io.BytesIO | tempfile.SpooledTemporaryFile[bytes] = io.BytesIO()
        else:
            # SpooledTemporaryFile(max_size=0) means "never roll to disk" in
            # CPython (it behaves like an unbounded in-memory buffer).  We
            # want spool_max_bytes=0 to mean "always use disk", so we map 0
            # to 1: a threshold of 1 byte causes a roll on the very first
            # write() call.
            effective_max_size: int = 1 if self._spool_max_bytes == 0 else self._spool_max_bytes
            out = tempfile.SpooledTemporaryFile(
                max_size=effective_max_size,  # type: ignore[arg-type]
                mode="w+b",
                dir="/tmp",
            )

        with TerseDecompresser.create(
            self._source,
            out,  # type: ignore[arg-type]
            text_mode=self._text_mode,
            max_output_bytes=self._max_output_bytes,
        ) as d:
            d.decode()

        out.seek(0)
        self._buffer = out

    # ------------------------------------------------------------------
    # io.RawIOBase interface
    # ------------------------------------------------------------------

    def readable(self) -> bool:
        """Return True — TerseFile is always readable."""
        return True

    def seekable(self) -> bool:
        """Return True — seeking is supported after decompression."""
        return True

    def read(self, size: int = -1) -> bytes:
        """Read and return up to *size* decompressed bytes.

        Args:
            size: Number of bytes to read.  -1 (default) reads all remaining
                  bytes.

        Returns:
            Decompressed bytes.
        """
        self._ensure_decompressed()
        if self._buffer is None:
            raise RuntimeError("Internal buffer is unexpectedly None after decompression")
        return self._buffer.read(size)

    def readinto(self, b: bytearray | memoryview) -> int:
        """Read bytes into a pre-allocated buffer.

        Args:
            b: A writable buffer (bytearray or memoryview).

        Returns:
            Number of bytes actually read.
        """
        self._ensure_decompressed()
        if self._buffer is None:
            raise RuntimeError("Internal buffer is unexpectedly None after decompression")
        data: bytes = self._buffer.read(len(b))
        n = len(data)
        b[:n] = data
        return n

    def seek(self, offset: int, whence: int = 0) -> int:
        """Seek to a position in the decompressed stream.

        Args:
            offset: Byte offset.
            whence: 0 = from start, 1 = from current, 2 = from end.

        Returns:
            New absolute position.
        """
        self._ensure_decompressed()
        if self._buffer is None:
            raise RuntimeError("Internal buffer is unexpectedly None after decompression")
        return self._buffer.seek(offset, whence)

    def tell(self) -> int:
        """Return the current position in the decompressed stream."""
        self._ensure_decompressed()
        if self._buffer is None:
            raise RuntimeError("Internal buffer is unexpectedly None after decompression")
        return self._buffer.tell()

    def close(self) -> None:
        """Close internal buffer (deletes any /tmp spill file) and, if owned, the source stream."""
        if not self.closed:
            if self._buffer is not None:
                self._buffer.close()
                self._buffer = None
            if self._close_source:
                self._source.close()
        super().close()
