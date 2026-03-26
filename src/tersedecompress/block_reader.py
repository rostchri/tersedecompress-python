"""
12-bit block reader for TERSE compressed streams.

Reads 12-bit values packed into a byte stream (two 12-bit values per 3 bytes).
Ported from TerseBlockReader.java.
"""

import io
from typing import BinaryIO

from .constants import ENDOFFILE


class TerseBlockReader:
    """Reads 12-bit blocks from a binary stream.

    The format packs two 12-bit values into 3 bytes:
      - First call: reads 2 bytes, returns high 12 bits, saves low 4 bits
      - Second call: reads 1 more byte, combines saved 4 bits + 8 new bits

    The underlying stream is wrapped in a BufferedReader if it does not
    already provide buffering, to avoid per-byte syscall overhead.
    """

    _BUFFER_SIZE: int = 8192

    def __init__(self, stream: BinaryIO) -> None:
        # Wrap in BufferedReader only when the stream has no internal buffer.
        # io.RawIOBase subclasses (e.g. FileIO) benefit from buffering;
        # BytesIO and BufferedReader already buffer internally.
        if isinstance(stream, io.RawIOBase):
            self._stream: BinaryIO = io.BufferedReader(stream, self._BUFFER_SIZE)  # type: ignore[arg-type]
        else:
            self._stream = stream
        self._bits_available: int = 0
        self._saved_bits: int = 0

    def get_blok(self) -> int:
        """Read 12 bits from the stream.

        Returns ENDOFFILE (0) when the stream is exhausted.

        Raises:
            IOError: If the stream ends unexpectedly mid-block.
        """
        if self._bits_available == 0:
            raw = self._stream.read(1)
            if not raw:
                return ENDOFFILE
            byte1 = raw[0]

            raw = self._stream.read(1)
            if not raw:
                raise IOError(
                    "Tried to read 12 bits but found EOF after reading 8 bits."
                )
            byte2 = raw[0]

            # Save the last 4 bits of the second byte
            self._saved_bits = byte2 & 0x0F
            self._bits_available = 4

            return (byte1 << 4) | (byte2 >> 4)
        else:
            if self._bits_available != 4:
                raise IOError("Unexpected count of bits available")

            raw = self._stream.read(1)
            if not raw:
                # Assume the 4 bits in the last block were the last real data;
                # these 4 bits only exist because you can't write 1/2 a byte.
                return ENDOFFILE

            self._bits_available = 0
            return (self._saved_bits << 8) | raw[0]

    def close(self) -> None:
        """Close the underlying stream."""
        self._stream.close()
