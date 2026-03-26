"""
High-level convenience functions for TERSE decompression.
"""

import io
from pathlib import Path
from typing import Union

from .base import TerseDecompresser


def decompress(
    data: bytes,
    *,
    text_mode: bool = False,
    max_output_bytes: int | None = None,
) -> bytes:
    """Decompress a TERSE-compressed byte string.

    Args:
        data:             The raw tersed bytes (including header).
        text_mode:        If True, perform EBCDIC→ASCII conversion and write
                          newlines after each logical record.  If False (default),
                          write raw binary data.
        max_output_bytes: If set, raise IOError when the total decompressed
                          output would exceed this many bytes.  None (default)
                          means no limit.

    Returns:
        The decompressed bytes.
    """
    in_stream = io.BytesIO(data)
    out_stream = io.BytesIO()

    with TerseDecompresser.create(
        in_stream, out_stream, text_mode=text_mode, max_output_bytes=max_output_bytes
    ) as d:
        d.decode()

    return out_stream.getvalue()


def decompress_file(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    *,
    text_mode: bool = False,
    max_output_bytes: int | None = None,
) -> None:
    """Decompress a TERSE-compressed file.

    Args:
        input_path:       Path to the tersed input file.
        output_path:      Path where decompressed data will be written.
        text_mode:        If True, perform EBCDIC→ASCII conversion.
        max_output_bytes: If set, raise IOError when the total decompressed
                          output would exceed this many bytes.  None (default)
                          means no limit.
    """
    input_path = Path(input_path)
    output_path = Path(output_path)

    with input_path.open("rb") as in_f, output_path.open("wb") as out_f:
        with TerseDecompresser.create(
            in_f, out_f, text_mode=text_mode, max_output_bytes=max_output_bytes
        ) as d:
            d.decode()
