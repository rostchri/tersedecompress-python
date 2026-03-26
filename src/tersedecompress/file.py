"""File-like interface for reading TERSE-compressed data."""

import io
from pathlib import Path
from typing import BinaryIO

from .core import decompress


class TerseFile(io.RawIOBase):
    """Read-only file-like wrapper that decompresses TERSE data on access.

    Decompresses the entire input eagerly into an internal buffer on first
    read. Supports read(), readinto(), seek(), tell(), and context manager.

    Usage:
        with TerseFile(open("data.tersed", "rb")) as f:
            content = f.read()
    """

    def __init__(
        self,
        source: BinaryIO,
        text_mode: bool = False,
        max_output_bytes: int | None = None,
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
            _close_source:    Internal flag — set to True when TerseFile owns
                              the stream and must close it on close().
        """
        super().__init__()
        self._source: BinaryIO = source
        self._text_mode: bool = text_mode
        self._max_output_bytes: int | None = max_output_bytes
        self._close_source: bool = _close_source
        self._buffer: io.BytesIO | None = None  # populated lazily on first read

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_decompressed(self) -> None:
        """Decompress source into the internal buffer if not done yet.

        Raises ValueError if the file is already closed.
        """
        if self.closed:
            raise ValueError("I/O operation on closed file")
        if self._buffer is None:
            raw: bytes = self._source.read()
            decompressed: bytes = decompress(
                raw,
                text_mode=self._text_mode,
                max_output_bytes=self._max_output_bytes,
            )
            self._buffer = io.BytesIO(decompressed)

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
        """Close internal buffer and, if owned, the source stream."""
        if not self.closed:
            if self._buffer is not None:
                self._buffer.close()
                self._buffer = None
            if self._close_source:
                self._source.close()
        super().close()
