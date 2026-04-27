"""
tersedecompress — Python port of IBM z/OS TERSE decompression.

Public API::

    from tersedecompress import decompress, decompress_file

    # Decompress bytes → bytes
    output = decompress(input_bytes)

    # Decompress a file (text mode: EBCDIC → ASCII + newline conversion)
    decompress_file("input.tersed", "output.txt", text_mode=True)

    # Seekable file-like API (analogous to gzip.open):
    with tersedecompress.open("input.tersed") as f:
        content = f.read()

    # Streaming forward-only API (no temp file, no seek):
    with tersedecompress.open("input.tersed", streaming=True) as f:
        shutil.copyfileobj(f, output_stream)
"""

import builtins
from pathlib import Path
from typing import BinaryIO

from .base import TerseDecompresser
from .core import decompress, decompress_file
from .file import TerseFile, TerseStreamFile


def open(
    source: str | Path | BinaryIO,
    text_mode: bool = False,
    max_output_bytes: int | None = None,
    streaming: bool = False,
    chunk_buffer_count: int = 64,
) -> TerseFile | TerseStreamFile:
    """Open a TERSE-compressed file for reading, analogous to gzip.open().

    Args:
        source:             File path (str/Path) or an already-open binary
                            stream to decompress.
        text_mode:          If True, convert EBCDIC to ASCII during
                            decompression.
        max_output_bytes:   Maximum decompressed size (None = unlimited).
        streaming:          If True, return a :class:`TerseStreamFile` —
                            a forward-only, no-seek, no-temp-file stream that
                            decompresses on the fly in a background thread.
                            Ideal for ``shutil.copyfileobj``, piping to a
                            subprocess stdin, or any single-pass consumer.
                            If False (default), return a seekable
                            :class:`TerseFile` backed by an in-memory buffer.
        chunk_buffer_count: Maximum number of decompressed chunks to buffer
                            in the internal queue when *streaming=True*.
                            Ignored when *streaming=False*.  Default: 64.

    Returns:
        :class:`TerseFile` when *streaming=False* (default), or
        :class:`TerseStreamFile` when *streaming=True*.

    Example::

        # Seekable (default):
        with tersedecompress.open("data.tersed") as f:
            content = f.read()

        # Streaming forward-only (no temp file, no seek):
        with tersedecompress.open("data.tersed", streaming=True) as f:
            shutil.copyfileobj(f, output_stream)

        with tersedecompress.open("data.tersed", streaming=True) as f:
            for chunk in iter(lambda: f.read(4096), b""):
                process(chunk)
    """
    close_source = isinstance(source, (str, Path))
    if close_source:
        stream: BinaryIO = builtins.open(Path(source), "rb")  # type: ignore[assignment]
    else:
        stream = source  # type: ignore[assignment]

    try:
        if streaming:
            return TerseStreamFile(
                stream,
                text_mode=text_mode,
                max_output_bytes=max_output_bytes,
                chunk_buffer_count=chunk_buffer_count,
                _close_source=close_source,
            )
        return TerseFile(
            stream,
            text_mode=text_mode,
            max_output_bytes=max_output_bytes,
            _close_source=close_source,
        )
    except Exception:
        if close_source:
            stream.close()
        raise


__all__ = [
    "TerseDecompresser",
    "TerseFile",
    "TerseStreamFile",
    "decompress",
    "decompress_file",
    "open",
]

__version__ = "1.0.0"
