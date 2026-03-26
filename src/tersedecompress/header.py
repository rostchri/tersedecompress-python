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

        raw = stream.read(1)
        if not raw:
            raise IOError("Unexpected EOF reading header version byte")
        header.version_flag = raw[0]

        if header.version_flag in (0x01, 0x07):
            # Native binary mode, 4-byte header, v1.1–1.2+
            rest = stream.read(3)
            if len(rest) < 3:
                raise IOError("Unexpected EOF reading header validation bytes")
            byte2, byte3, byte4 = rest[0], rest[1], rest[2]

            raw2 = stream.read(2)
            if len(raw2) < 2:
                raise IOError("Unexpected EOF reading RecordLen1")
            header.record_len1 = struct.unpack(">H", raw2)[0]

            if byte2 != 0x89 or byte3 != 0x69 or byte4 != 0xA5:
                raise IOError("Invalid header validation flags")

            header.host_flag = False  # auto-switch to native mode
            header.text_flag = False

        elif header.version_flag in (0x02, 0x05):
            # Host PACK (0x02) or SPACK (0x05) compatibility mode, 12-byte header
            raw1 = stream.read(1)
            if not raw1:
                raise IOError("Unexpected EOF reading VariableFlag")
            header.variable_flag = raw1[0]

            raw2 = stream.read(2)
            if len(raw2) < 2:
                raise IOError("Unexpected EOF reading RecordLen1")
            header.record_len1 = struct.unpack(">H", raw2)[0]

            raw3 = stream.read(1)
            if not raw3:
                raise IOError("Unexpected EOF reading Flags")
            header.flags = raw3[0]

            raw4 = stream.read(1)
            if not raw4:
                raise IOError("Unexpected EOF reading Ratio")
            header.ratio = raw4[0]

            raw5 = stream.read(2)
            if len(raw5) < 2:
                raise IOError("Unexpected EOF reading BlockSize")
            header.block_size = struct.unpack(">H", raw5)[0]

            raw6 = stream.read(4)
            if len(raw6) < 4:
                raise IOError("Unexpected EOF reading RecordLen2")
            header.record_len2 = struct.unpack(">i", raw6)[0]  # signed 32-bit

            if header.record_len2 < 0:
                raise IOError(
                    f"Record length exceeds {2**31 - 1}"
                )

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

        else:
            raise IOError(
                f"Terse header version not recognized: {header.version_flag:#04x}"
            )

        return header
