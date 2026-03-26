"""
tersedecompress — Python port of IBM z/OS TERSE decompression.

Public API::

    from tersedecompress import decompress, decompress_file

    # Decompress bytes → bytes
    output = decompress(input_bytes)

    # Decompress a file (text mode: EBCDIC → ASCII + newline conversion)
    decompress_file("input.tersed", "output.txt", text_mode=True)
"""

from .base import TerseDecompresser
from .core import decompress, decompress_file

__all__ = [
    "TerseDecompresser",
    "decompress",
    "decompress_file",
]

__version__ = "1.0.0"
