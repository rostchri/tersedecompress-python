"""
High-level convenience functions for TERSE decompression.
"""

import io
from pathlib import Path
from typing import BinaryIO, Union

from . import fast
from .header import TerseHeader


def _decompress_stream(
    in_stream: BinaryIO,
    out_stream: BinaryIO,
    *,
    text_mode: bool | None = None,
    max_output_bytes: int | None = None,
) -> None:
    """Decompress *in_stream* into *out_stream*, using the C core when possible.

    Parses the header once, then dispatches to the optional Cython PACK
    accelerator (:mod:`tersedecompress.fast`) when it is available and the
    stream is PACK-encoded.  Otherwise it falls back to the pure-Python
    :class:`TerseDecompresser` — behaviour and output are identical either way.
    """
    header = TerseHeader.check_header(in_stream)
    if text_mode is not None:
        header.text_flag = text_mode

    if fast.can_accelerate(header):
        fast.run(header, in_stream, out_stream, max_output_bytes=max_output_bytes)
        out_stream.flush()
        return

    # Import here to avoid a circular import at module load time.
    from .pack import PackDecompresser
    from .spack import SpackDecompresser

    decoder_cls = SpackDecompresser if header.spack_flag else PackDecompresser
    with decoder_cls(
        in_stream, out_stream, header, max_output_bytes=max_output_bytes
    ) as d:
        d.decode()


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

    _decompress_stream(
        in_stream,
        out_stream,
        text_mode=text_mode,
        max_output_bytes=max_output_bytes,
    )

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
        _decompress_stream(
            in_f,
            out_f,
            text_mode=text_mode,
            max_output_bytes=max_output_bytes,
        )
