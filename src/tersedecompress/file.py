"""File-like interface for reading TERSE-compressed data."""

import io
import queue
import threading
from pathlib import Path
from typing import BinaryIO

from .base import TerseDecompresser
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


# ---------------------------------------------------------------------------
# Streaming (forward-only, no-seek, no-temp-file) variant
# ---------------------------------------------------------------------------

_QUEUE_SENTINEL: None = None  # typed alias for the EOF marker put on the queue


class _QueueWriter:
    """File-like sink that puts written bytes onto a :class:`queue.Queue`.

    Used as the *out_stream* for :class:`~tersedecompresser.base.TerseDecompresser`
    inside the producer thread.  The queue provides automatic back-pressure: once
    the queue reaches *maxsize* the producer blocks on ``put()`` until the
    consumer drains a slot.

    The writer is intentionally minimal — only ``write()`` and ``flush()``
    are needed by the decompressor.
    """

    def __init__(
        self,
        q: "queue.Queue[bytes | None]",
        cancel_event: threading.Event,
    ) -> None:
        self._queue = q
        self._cancel = cancel_event

    def write(self, data: bytes) -> int:
        """Enqueue *data*, blocking until space is available or cancelled."""
        if not data:
            return 0
        # Block until either there is room in the queue or the consumer has
        # signalled cancellation (e.g. the TerseStreamFile was closed early).
        while not self._cancel.is_set():
            try:
                self._queue.put(data, timeout=0.05)
                return len(data)
            except queue.Full:
                continue
        # Cancelled — raise so the producer thread exits promptly.
        raise IOError("TerseStreamFile: cancelled by consumer")

    def flush(self) -> None:  # noqa: D401
        """No-op — queue items are consumed directly by the reader."""


class TerseStreamFile(io.RawIOBase):
    """Forward-only, no-seek, no-temp-file streaming decompressor.

    Decompresses TERSE data on the fly using a background producer thread
    that feeds decompressed chunks into a bounded :class:`queue.Queue`.
    The calling thread consumes those chunks via :meth:`read` /
    :meth:`readinto`.

    Key properties:

    * **No seekability** — :meth:`seekable` returns ``False``;
      :meth:`seek` and :meth:`tell` raise :exc:`io.UnsupportedOperation`.
    * **No temp file** — decompressed bytes are never written to disk.
    * **Back-pressure via queue** — the producer blocks when the queue is
      full, keeping peak memory proportional to
      ``chunk_buffer_count × average_chunk_size``.
    * **Error propagation** — exceptions from the producer thread are
      re-raised in the consumer thread on the next :meth:`read` call.
    * **Clean shutdown** — :meth:`close` signals the producer to stop and
      joins the thread with a short timeout; the thread is a daemon thread
      as a safety net.

    Usage::

        with TerseStreamFile(open("data.tersed", "rb")) as f:
            shutil.copyfileobj(f, output)

        # Or via the public API:
        with tersedecompress.open("data.tersed", streaming=True) as f:
            for chunk in iter(lambda: f.read(4096), b""):
                process(chunk)
    """

    def __init__(
        self,
        source: BinaryIO,
        text_mode: bool = False,
        max_output_bytes: int | None = None,
        chunk_buffer_count: int = 64,
        *,
        _close_source: bool = False,
    ) -> None:
        """Initialise TerseStreamFile and start the producer thread.

        Args:
            source:             An open binary stream containing
                                TERSE-compressed data.
            text_mode:          If True, perform EBCDIC→ASCII conversion
                                during decompression.
            max_output_bytes:   Upper bound on decompressed size.  None
                                means unlimited.
            chunk_buffer_count: Maximum number of decompressed chunks
                                buffered in the internal queue.  A larger
                                value reduces the chance of the producer
                                stalling but increases peak memory usage.
                                Default: 64.
            _close_source:      Internal flag — set to True when this
                                object owns the stream and must close it
                                on :meth:`close`.
        """
        super().__init__()
        self._source: BinaryIO = source
        self._text_mode: bool = text_mode
        self._max_output_bytes: int | None = max_output_bytes
        self._close_source: bool = _close_source

        # Queue carries chunks (bytes) or the EOF sentinel (None).
        self._queue: queue.Queue[bytes | None] = queue.Queue(maxsize=chunk_buffer_count)
        # Signals the producer to abort (set on close() before EOF).
        self._cancel: threading.Event = threading.Event()
        # Producer stores any exception here before putting the sentinel.
        self._error_box: list[BaseException] = []
        # Bytes leftover from the previous read() call when size < chunk size.
        self._leftover: bytes = b""
        self._eof: bool = False

        self._thread = threading.Thread(
            target=self._produce,
            name="TerseStreamFile-producer",
            daemon=True,
        )
        self._thread.start()

    # ------------------------------------------------------------------
    # Producer (runs in background thread)
    # ------------------------------------------------------------------

    def _produce(self) -> None:
        """Decompress the source and push chunks onto the queue.

        Runs entirely in the background thread.  Any exception is stored
        in ``_error_box``; the EOF sentinel is always enqueued last so
        that the consumer can detect end-of-stream.
        """
        writer = _QueueWriter(self._queue, self._cancel)
        try:
            with TerseDecompresser.create(
                self._source,
                writer,  # type: ignore[arg-type]
                text_mode=self._text_mode,
                max_output_bytes=self._max_output_bytes,
            ) as d:
                d.decode()
        except BaseException as exc:  # noqa: BLE001
            self._error_box.append(exc)
        finally:
            # Always enqueue the sentinel so the consumer unblocks.
            # If the queue is full and we are cancelled we still push the
            # sentinel with a blocking call — the consumer will drain one
            # slot when it detects cancellation, so we will not deadlock.
            while True:
                try:
                    self._queue.put(_QUEUE_SENTINEL, timeout=0.05)
                    break
                except queue.Full:
                    if self._cancel.is_set():
                        # Consumer is gone; discard remaining items so put()
                        # succeeds on the next iteration.
                        try:
                            self._queue.get_nowait()
                        except queue.Empty:
                            pass
            if self._close_source:
                try:
                    self._source.close()
                except Exception:  # noqa: BLE001
                    pass

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _check_closed(self) -> None:
        if self.closed:
            raise ValueError("I/O operation on closed file")

    def _next_chunk(self) -> bytes:
        """Return the next chunk from the queue, or ``b""`` on EOF.

        Checks ``_error_box`` after the sentinel is received and
        re-raises any producer exception in the consumer thread.
        """
        if self._eof:
            return b""
        chunk = self._queue.get()
        if chunk is _QUEUE_SENTINEL:
            self._eof = True
            if self._error_box:
                raise self._error_box[0]
            return b""
        return chunk  # type: ignore[return-value]

    def _fill_leftover(self) -> None:
        """Ensure ``_leftover`` is non-empty (or EOF has been reached)."""
        while not self._leftover and not self._eof:
            self._leftover = self._next_chunk()

    # ------------------------------------------------------------------
    # io.RawIOBase interface
    # ------------------------------------------------------------------

    def readable(self) -> bool:
        """Return True — TerseStreamFile is always readable."""
        return True

    def seekable(self) -> bool:
        """Return False — forward-only streaming, no seeking."""
        return False

    def seek(self, offset: int, whence: int = 0) -> int:  # noqa: D401
        """Always raises :exc:`io.UnsupportedOperation`."""
        raise io.UnsupportedOperation("seek")

    def tell(self) -> int:  # noqa: D401
        """Always raises :exc:`io.UnsupportedOperation`."""
        raise io.UnsupportedOperation("tell")

    def read(self, size: int = -1) -> bytes:
        """Read and return up to *size* decompressed bytes.

        Args:
            size: Number of bytes to read.  ``-1`` (default) reads all
                  remaining bytes.

        Returns:
            Decompressed bytes, or ``b""`` at end-of-stream.
        """
        self._check_closed()
        if size == 0:
            return b""
        if size < 0:
            # Read everything.
            parts: list[bytes] = []
            if self._leftover:
                parts.append(self._leftover)
                self._leftover = b""
            while True:
                chunk = self._next_chunk()
                if not chunk:
                    break
                parts.append(chunk)
            return b"".join(parts)

        # Sized read — assemble exactly *size* bytes from leftover + queue.
        parts = []
        remaining = size
        if self._leftover:
            if len(self._leftover) <= remaining:
                parts.append(self._leftover)
                remaining -= len(self._leftover)
                self._leftover = b""
            else:
                parts.append(self._leftover[:remaining])
                self._leftover = self._leftover[remaining:]
                remaining = 0

        while remaining > 0 and not self._eof:
            chunk = self._next_chunk()
            if not chunk:
                break
            if len(chunk) <= remaining:
                parts.append(chunk)
                remaining -= len(chunk)
            else:
                parts.append(chunk[:remaining])
                self._leftover = chunk[remaining:]
                remaining = 0

        return b"".join(parts)

    def readinto(self, b: bytearray | memoryview) -> int:
        """Read bytes into a pre-allocated buffer.

        Args:
            b: A writable buffer (bytearray or memoryview).

        Returns:
            Number of bytes actually read (0 at end-of-stream).
        """
        self._check_closed()
        data = self.read(len(b))
        n = len(data)
        b[:n] = data
        return n

    def close(self) -> None:
        """Signal the producer thread to stop, then join it."""
        if not self.closed:
            # Signal the producer to abort its queue.put() loop.
            self._cancel.set()
            # Drain the queue so the producer can put the sentinel and exit.
            while self._thread.is_alive():
                try:
                    self._queue.get_nowait()
                except queue.Empty:
                    pass
                self._thread.join(timeout=0.05)
            # Final drain in case items arrived after last check.
            while True:
                try:
                    self._queue.get_nowait()
                except queue.Empty:
                    break
            if self._close_source and not self._source.closed:
                self._source.close()
        super().close()
