"""
TERSE file header parsing.

Parses the fixed-size header at the beginning of a tersed file and
determines the decompression mode (PACK vs SPACK, binary vs text, etc.).
Ported from TerseHeader.java.
"""

import io
import struct
from dataclasses import dataclass, field
from typing import BinaryIO

from .constants import FLAGMVS


def _read_exact(stream: BinaryIO, n: int, field_name: str) -> bytes:
    """Read exactly *n* bytes from *stream*, raising IOError on short reads.

    Args:
        stream:     Binary stream to read from.
        n:          Number of bytes to read.
        field_name: Human-readable name used in the error message.

    Raises:
        IOError: If fewer than *n* bytes are available.
    """
    data = stream.read(n)
    if len(data) < n:
        raise IOError(f"Unexpected EOF reading {field_name}")
    return data


def _read_u8(stream: BinaryIO, field_name: str) -> int:
    """Read one unsigned byte from *stream*."""
    return _read_exact(stream, 1, field_name)[0]


def _read_u16_be(stream: BinaryIO, field_name: str) -> int:
    """Read a big-endian unsigned 16-bit integer from *stream*."""
    return struct.unpack(">H", _read_exact(stream, 2, field_name))[0]


def _read_i32_be(stream: BinaryIO, field_name: str) -> int:
    """Read a big-endian signed 32-bit integer from *stream*."""
    return struct.unpack(">i", _read_exact(stream, 4, field_name))[0]


@dataclass
class TerseHeader:
    """Parsed TERSE file header."""

    version_flag: int = 0
    variable_flag: int = 0
    record_len1: int = 0
    flags: int = 0
    ratio: int = 0
    block_size: int = 0
    record_len2: int = 0
    record_length: int = 0

    recfm_v: bool = False

    # Defaults for dump types
    text_flag: bool = True
    host_flag: bool = True
    spack_flag: bool = True

    def __str__(self) -> str:
        return (
            f"\nVersion flag is {self.version_flag}\n"
            f"Variable Flag is {self.variable_flag}\n"
            f"RecordLen1 is {self.record_len1}\n"
            f"Flags are {self.flags}\n"
            f"Ratio is {self.ratio}\n"
            f"Block Size is {self.block_size}\n"
            f"RecordLen2 is {self.record_len2}\n"
        )

    @staticmethod
    def _parse_native_header(stream: BinaryIO, header: "TerseHeader") -> None:
        """Parse a native (non-host) TERSE header (version 0x01 or 0x07).

        Reads the 3 validation bytes and the 2-byte RecordLen1 field.

        Args:
            stream: Binary stream positioned directly after the version byte.
            header: Header instance to populate in-place.

        Raises:
            IOError: On short read or invalid validation bytes.
        """
        validation = _read_exact(stream, 3, "header validation bytes")
        byte2, byte3, byte4 = validation[0], validation[1], validation[2]
        header.record_len1 = _read_u16_be(stream, "RecordLen1")

        if byte2 != 0x89 or byte3 != 0x69 or byte4 != 0xA5:
            raise IOError("Invalid header validation flags")

        header.host_flag = False
        header.text_flag = False

    @staticmethod
    def _parse_host_header(stream: BinaryIO, header: "TerseHeader") -> None:
        """Parse a host-mode TERSE header (version 0x02 or 0x05).

        Reads the 11 bytes that follow the version byte, validates them, and
        populates all relevant fields on *header*.

        Args:
            stream: Binary stream positioned directly after the version byte.
            header: Header instance to populate in-place.

        Raises:
            IOError: On short read or any semantic validation failure.
        """
        header.variable_flag = _read_u8(stream, "VariableFlag")
        header.record_len1 = _read_u16_be(stream, "RecordLen1")
        header.flags = _read_u8(stream, "Flags")
        header.ratio = _read_u8(stream, "Ratio")
        header.block_size = _read_u16_be(stream, "BlockSize")
        header.record_len2 = _read_i32_be(stream, "RecordLen2")

        if header.record_len2 < 0:
            raise IOError(f"Record length exceeds {2**31 - 1}")

        header.spack_flag = header.version_flag == 0x05

        if header.variable_flag not in (0x00, 0x01):
            raise IOError(
                f"Record format flag not recognized: "
                f"{header.variable_flag:#04x}"
            )

        if header.record_len1 == 0 and header.record_len2 == 0:
            raise IOError("Record length is 0")

        if (
            header.record_len1 != 0
            and header.record_len2 != 0
            and header.record_len1 != header.record_len2
        ):
            raise IOError("Ambiguous record length")

        header.record_length = (
            header.record_len1 if header.record_len1 != 0 else header.record_len2
        )

        header.recfm_v = header.variable_flag == 0x01

        if (header.flags & FLAGMVS) == 0:
            if header.flags != 0:
                raise IOError("Flags specified for non-MVS")
            if header.ratio != 0:
                raise IOError("Ratio specified for non-MVS")
            if header.block_size != 0:
                raise IOError("BlockSize specified for non-MVS")

        header.host_flag = True

    @staticmethod
    def check_header(stream: BinaryIO) -> "TerseHeader":
        """Read and validate the TERSE file header from *stream*.

        Args:
            stream: A binary stream positioned at the start of the tersed data.

        Returns:
            A populated TerseHeader instance.

        Raises:
            IOError: If the header is missing, invalid, or uses an unsupported format.
        """
        header = TerseHeader()
        header.version_flag = _read_u8(stream, "header version byte")

        if header.version_flag in (0x01, 0x07):
            TerseHeader._parse_native_header(stream, header)
        elif header.version_flag in (0x02, 0x05):
            TerseHeader._parse_host_header(stream, header)
        else:
            raise IOError(
                f"Terse header version not recognized: {header.version_flag:#04x}"
            )

        return header
