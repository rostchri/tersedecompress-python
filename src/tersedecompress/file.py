"""File-like interface for reading TERSE-compressed data."""

import io
import queue
import threading
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

    def _ensure_decompressed(self) -> io.BytesIO:
        """Decompress source into the internal buffer if not done yet.

        Returns:
            The internal BytesIO buffer (guaranteed non-None).

        Raises:
            ValueError: If the file is already closed.
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
        return self._buffer

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
        return self._ensure_decompressed().read(size)

    def readinto(self, b: bytearray | memoryview) -> int:
        """Read bytes into a pre-allocated buffer.

        Args:
            b: A writable buffer (bytearray or memoryview).

        Returns:
            Number of bytes actually read.
        """
        buf = self._ensure_decompressed()
        data: bytes = buf.read(len(b))
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
        return self._ensure_decompressed().seek(offset, whence)

    def tell(self) -> int:
        """Return the current position in the decompressed stream."""
        return self._ensure_decompressed().tell()

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

# Poll interval for queue operations.  50 ms is a reasonable balance between
# shutdown latency and CPU idle cost.
_QUEUE_TIMEOUT: float = 0.05


# Sentinel raised internally to signal cancellation; never stored in _error_box.
class _CancelledError(Exception):
    """Internal signal: producer was cancelled by close()."""


class _QueueWriter:
    """File-like sink that puts written bytes onto a :class:`queue.Queue`.

    Used as the *out_stream* for
    :class:`~tersedecompress.base.TerseDecompresser` inside the producer
    thread.  The queue provides automatic back-pressure: once the queue
    reaches *maxsize* the producer blocks on ``put()`` until the consumer
    drains a slot.

    The writer is intentionally minimal — only ``write()`` and ``flush()``
    are needed by the decompressor.

    Note on polling: ``write()`` polls the queue every ``_QUEUE_TIMEOUT``
    seconds rather than blocking indefinitely so that ``_cancel`` can be
    detected promptly on ``close()``.  The idle CPU cost is negligible for
    typical throughput.
    """

    def __init__(
        self,
        q: "queue.Queue[bytes | None]",
        cancel_event: threading.Event,
    ) -> None:
        self._queue = q
        self._cancel = cancel_event

    def write(self, data: bytes) -> int:
        """Enqueue *data*, blocking until space is available or cancelled.

        Raises:
            _CancelledError: If the consumer signals cancellation via the
                cancel event before a queue slot becomes available.
        """
        if not data:
            return 0
        while not self._cancel.is_set():
            try:
                self._queue.put(data, timeout=_QUEUE_TIMEOUT)
                return len(data)
            except queue.Full:
                continue
        # Raise a private sentinel so _produce() can distinguish
        # cancellation from a real decompression error.
        raise _CancelledError()

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
      After an error the stream is in a *broken* state; subsequent reads
      raise :exc:`RuntimeError`.
    * **Clean shutdown** — :meth:`close` signals the producer to stop and
      joins the thread with a short timeout; the thread is a daemon thread
      as a safety net so interpreter shutdown is never blocked.

    Source ownership:

    * When *_close_source* is ``True`` the **producer thread** is the sole
      owner and closes the source in its ``finally`` block.  ``close()``
      must not also call ``source.close()`` to avoid a race.

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
                                object owns the stream.  The **producer
                                thread** will close the source; ``close()``
                                does not, to avoid a race.
        """
        super().__init__()
        self._source: BinaryIO = source
        self._text_mode: bool = text_mode
        self._max_output_bytes: int | None = max_output_bytes
        self._close_source: bool = _close_source

        # Queue carries chunks (bytes) or the EOF sentinel (None).
        self._queue: queue.Queue[bytes | None] = queue.Queue(maxsize=chunk_buffer_count)
        # Set by close() to abort the producer's queue.put() poll loop.
        self._cancel: threading.Event = threading.Event()
        # Producer stores real (non-cancellation) exceptions here.
        # Written once by the producer thread before the sentinel is enqueued,
        # read once by the consumer after the sentinel is dequeued.
        # The queue itself provides the happens-before guarantee, so no lock
        # is needed in CPython.  Under free-threaded Python 3.13 the list
        # append / index-0 access is also atomic for single-element lists.
        self._error_box: list[Exception] = []
        # Set to True once the sentinel has been dequeued; broken if error.
        self._eof: bool = False
        self._broken: bool = False
        # Bytes leftover from the previous read() call when size < chunk size.
        self._leftover: bytes = b""

        # daemon=True is a safety net: if the consumer is garbage-collected
        # without calling close(), the producer will not block interpreter
        # shutdown.  Proper cleanup relies on close() / the context manager.
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

        Runs entirely in the background thread.  Real decompression
        exceptions are stored in ``_error_box``; cancellation (_CancelledError)
        is silently swallowed so it never reaches ``_error_box``.  The EOF
        sentinel is always enqueued last so that the consumer unblocks.
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
        except _CancelledError:
            # Normal early-close path — not an error; do not store.
            pass
        except Exception as exc:
            # Real decompression / IO error; re-raise on the consumer side.
            self._error_box.append(exc)
        finally:
            # Always enqueue the sentinel so the consumer unblocks.
            # When cancelled the consumer is draining the queue, so we
            # discard stale items to make room for the sentinel.
            while True:
                try:
                    self._queue.put(_QUEUE_SENTINEL, timeout=_QUEUE_TIMEOUT)
                    break
                except queue.Full:
                    if self._cancel.is_set():
                        try:
                            self._queue.get_nowait()
                        except queue.Empty:
                            pass
            # Close source only once, here in the producer, when we own it.
            if self._close_source:
                try:
                    self._source.close()
                except Exception:  # noqa: BLE001 — best-effort; errors are non-actionable
                    pass

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _check_closed(self) -> None:
        if self.closed:
            raise ValueError("I/O operation on closed file")

    def _next_chunk(self) -> bytes:
        """Return the next chunk from the queue, or ``b""`` on EOF.

        Blocks with a timeout and re-checks ``_cancel`` to avoid a permanent
        block if the producer died without enqueuing the sentinel (e.g. if
        the process receives SIGKILL while the producer is blocked on I/O).

        After the sentinel is received:
        - If a real error occurred, sets ``_broken`` and re-raises the
          exception on every subsequent call.
        - Otherwise sets ``_eof`` and returns ``b""``.

        Raises:
            Exception: Any decompression error stored by the producer.
            RuntimeError: If called again after an error was raised.
        """
        if self._broken:
            raise RuntimeError("TerseStreamFile: stream is broken due to a prior error")
        if self._eof:
            return b""
        while True:
            try:
                chunk = self._queue.get(timeout=_QUEUE_TIMEOUT)
                break
            except queue.Empty:
                # Re-check whether the producer is still alive.  If it has
                # exited without enqueuing the sentinel (abnormal), treat it
                # as EOF so the consumer is never permanently blocked.
                if not self._thread.is_alive() and self._queue.empty():
                    if self._error_box:
                        self._broken = True
                        raise self._error_box[0]
                    self._eof = True
                    return b""
        if chunk is _QUEUE_SENTINEL:
            if self._error_box:
                self._broken = True
                raise self._error_box[0]
            self._eof = True
            return b""
        return chunk  # type: ignore[return-value]

    def _read_all(self) -> bytes:
        """Read all remaining bytes from the queue."""
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

    def _read_sized(self, size: int) -> bytes:
        """Read exactly *size* bytes (or less if EOF is reached)."""
        parts: list[bytes] = []
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

        while remaining > 0 and not self._eof and not self._broken:
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

        Raises:
            ValueError: If the file is closed.
            RuntimeError: If the stream is broken due to a prior error.
            Exception: Any decompression error propagated from the producer.
        """
        self._check_closed()
        if size == 0:
            return b""
        if size < 0:
            return self._read_all()
        return self._read_sized(size)

    def readinto(self, b: bytearray | memoryview) -> int:
        """Read bytes into a pre-allocated buffer.

        Note: this implementation allocates an intermediate ``bytes`` object.
        For allocation-critical paths, prefer ``read()`` directly into a
        ``bytearray``.

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
        """Signal the producer thread to stop and join it.

        Source stream ownership: when *_close_source* is True the producer
        thread is the sole owner and closes the source itself.  ``close()``
        must not additionally call ``source.close()`` to avoid a race.
        """
        if not self.closed:
            # Signal the producer to abort its queue.put() poll loop.
            self._cancel.set()
            # Drain the queue so the producer can enqueue the sentinel and exit.
            while self._thread.is_alive():
                try:
                    self._queue.get_nowait()
                except queue.Empty:
                    pass
                self._thread.join(timeout=_QUEUE_TIMEOUT)
            # Final drain in case a last item arrived between the loop check
            # and the thread exiting.
            while True:
                try:
                    self._queue.get_nowait()
                except queue.Empty:
                    break
            # Source is closed by the producer when _close_source=True.
            # When _close_source=False the caller owns the source; do not touch it.
        super().close()
