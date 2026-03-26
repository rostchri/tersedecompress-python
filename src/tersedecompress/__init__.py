"""
tersedecompress — Python port of IBM z/OS TERSE decompression.

Public API::

    from tersedecompress import decompress, decompress_file

    # Decompress bytes → bytes
    output = decompress(input_bytes)

    # Decompress a file (text mode: EBCDIC → ASCII + newline conversion)
    decompress_file("input.tersed", "output.txt", text_mode=True)

    # File-like API (analogous to gzip.open)
    with tersedecompress.open("input.tersed") as f:
        content = f.read()
"""

import builtins
from pathlib import Path
from typing import BinaryIO, Union

from .base import TerseDecompresser
from .core import decompress, decompress_file
from .file import TerseFile


def open(
    source: Union[str, Path, BinaryIO],
    text_mode: bool = False,
    max_output_bytes: int | None = None,
) -> TerseFile:
    """Open a TERSE-compressed file for reading, analogous to gzip.open().

    Args:
        source:           File path (str/Path) or an already-open binary stream
                          to decompress.
        text_mode:        If True, convert EBCDIC to ASCII during decompression.
        max_output_bytes: Maximum decompressed size (None = unlimited).

    Returns:
        TerseFile: A readable, seekable file-like object with decompressed
        content.

    Example:
        with tersedecompress.open("data.tersed") as f:
            content = f.read()

        with tersedecompress.open("data.tersed") as f:
            while chunk := f.read(4096):
                process(chunk)
    """
    if isinstance(source, (str, Path)):
        stream: BinaryIO = builtins.open(Path(source), "rb")  # type: ignore[assignment]
        return TerseFile(
            stream,
            text_mode=text_mode,
            max_output_bytes=max_output_bytes,
            _close_source=True,
        )
    return TerseFile(
        source,
        text_mode=text_mode,
        max_output_bytes=max_output_bytes,
        _close_source=False,
    )


__all__ = [
    "TerseDecompresser",
    "TerseFile",
    "decompress",
    "decompress_file",
    "open",
]

__version__ = "1.0.0"
