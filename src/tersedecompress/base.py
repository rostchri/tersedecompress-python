"""
Base decompressor with record output logic.

Handles the PutChar / endRecord logic that is shared between
PACK and SPACK decompressors.  Ported from TerseDecompresser.java.
"""

import io
import struct
from abc import ABC, abstractmethod
from typing import BinaryIO

from .block_reader import TerseBlockReader
from .constants import EBC_TO_ASC, RECORDMARK
from .header import TerseHeader


class TerseDecompresser(ABC):
    """Abstract base class for TERSE decompressors.

    Sub-classes implement :meth:`decode` for the specific algorithm
    (PACK or SPACK).  Output logic (record assembly, EBCDIC→ASCII
    conversion, RDW writing for VB binary) lives here.
    """

    def __init__(
        self,
        in_stream: BinaryIO,
        out_stream: BinaryIO,
        header: TerseHeader,
    ) -> None:
        self.record_length: int = header.record_length
        self.host_flag: bool = header.host_flag
        self.text_flag: bool = header.text_flag
        self.variable_flag: bool = header.recfm_v

        self._input = TerseBlockReader(in_stream)
        self._out = out_stream
        self._record = bytearray()

        self._line_separator: bytes = b"\n"

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    def decode(self) -> None:
        """Decompress the input stream and write to the output stream."""

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def create(
        cls,
        in_stream: BinaryIO,
        out_stream: BinaryIO,
    ) -> "TerseDecompresser":
        """Parse the header and return the appropriate decompressor.

        Args:
            in_stream:  Opened binary input stream (tersed data).
            out_stream: Opened binary output stream (decompressed data).

        Returns:
            A :class:`PackDecompresser` or :class:`SpackDecompresser` instance.
        """
        # Import here to avoid circular imports
        from .pack import PackDecompresser
        from .spack import SpackDecompresser

        header = TerseHeader.check_header(in_stream)

        if not header.spack_flag:
            return PackDecompresser(in_stream, out_stream, header)
        else:
            return SpackDecompresser(in_stream, out_stream, header)

    # ------------------------------------------------------------------
    # Output helpers
    # ------------------------------------------------------------------

    def end_record(self) -> None:
        """Flush the current record buffer to the output stream.

        For VB binary mode, writes a 4-byte RDW (Record Descriptor Word)
        before the record data.  For text mode, appends the line separator.
        """
        if self.variable_flag and not self.text_flag:
            # Write a RDW: length = record_data_length + 4 (includes RDW itself)
            # Java: int rdw = recordlength << 16  (top 16 bits = length, low 16 = 0)
            record_length_with_rdw = len(self._record) + 4
            rdw = (record_length_with_rdw << 16) & 0xFFFFFFFF
            self._out.write(struct.pack(">I", rdw))

        self._out.write(bytes(self._record))
        self._record = bytearray()

        if self.text_flag:
            self._out.write(self._line_separator)

    def put_char(self, x: int) -> None:
        """Write a single logical character to the current record.

        *x* is the 1-based EBCDIC codepoint (0 = end-of-stream marker,
        257 = RECORDMARK).

        Args:
            x: Value in the range 0..257.
        """
        if x == 0:
            if self.host_flag and self.text_flag and self.variable_flag:
                self.end_record()
        else:
            if self.host_flag and self.text_flag:
                if self.variable_flag:
                    if x == RECORDMARK:
                        self.end_record()
                    else:
                        self._record.append(EBC_TO_ASC[x - 1])
                else:
                    self._record.append(EBC_TO_ASC[x - 1])
                    if len(self._record) == self.record_length:
                        self.end_record()
            else:
                if x == RECORDMARK:
                    if self.variable_flag:
                        self.end_record()
                    # else: discard record marks
                else:
                    self._record.append(x - 1)

    def close(self) -> None:
        """Flush any remaining data and close streams."""
        if self._record or (self.text_flag and self.variable_flag):
            self.end_record()
        self._out.flush()
        self._input.close()

    # ------------------------------------------------------------------
    # Context-manager support
    # ------------------------------------------------------------------

    def __enter__(self) -> "TerseDecompresser":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
