"""
Optional Cython acceleration dispatcher.

The compiled extension :mod:`tersedecompress._terse_fast` is OPTIONAL.  When it
is present (built via ``setup_fast.py``), both PACK and SPACK decompression run
the C core and stream their output to the target stream.  When it is absent,
callers transparently fall back to the pure-Python decoder.

Nothing here imports the extension at call time on the hot path; the import is
attempted exactly once at module load.
"""

from typing import BinaryIO

from .constants import ASC_TO_EBC_DEF, EBC_TO_ASC
from .header import TerseHeader

try:
    from . import _terse_fast  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover - depends on whether the .so was built
    _terse_fast = None  # type: ignore[assignment]

# Pre-built once; passed to the C core on every call.
_EBC_TO_ASC: bytes = bytes(EBC_TO_ASC)
_SPACE_CODE: int = 1 + ASC_TO_EBC_DEF[ord(" ")]


def is_available() -> bool:
    """Return True if the compiled accelerator is importable."""
    return _terse_fast is not None


def can_accelerate(header: TerseHeader) -> bool:
    """Return True if the C core can decode *header* (PACK or SPACK)."""
    return _terse_fast is not None


def run(
    header: TerseHeader,
    in_stream: BinaryIO,
    out_stream: BinaryIO,
    *,
    max_output_bytes: int | None = None,
) -> int:
    """Decode a PACK or SPACK stream with the C core, writing to *out_stream*.

    The header must already have been parsed from *in_stream* (so the stream is
    positioned at the start of the compressed payload).  ``header.text_flag``
    selects EBCDIC→ASCII text mode, mirroring the pure-Python path; the
    algorithm (PACK vs SPACK) is chosen from ``header.spack_flag``.

    Returns the number of decompressed bytes written.

    Raises:
        RuntimeError: If called when no accelerator is available.  Callers
                      should gate on :func:`can_accelerate` first.
    """
    if _terse_fast is None:  # pragma: no cover - guarded by can_accelerate()
        raise RuntimeError("Cython accelerator is not available")

    payload = in_stream.read()
    decode = _terse_fast.decode_spack if header.spack_flag else _terse_fast.decode_pack
    return decode(
        payload,
        header.record_length,
        header.host_flag,
        header.text_flag,
        header.recfm_v,
        _SPACE_CODE,
        _EBC_TO_ASC,
        out_stream,
        max_output_bytes,
    )
