"""
PACK decompressor (LZW variant used by IBM z/OS TERSE).

Ported exactly from NonSpackDecompresser.java.
Uses four parallel integer arrays (Father, CharExt, Backward, Forward)
together with a 12-bit block reader.
"""

from typing import BinaryIO

from .base import TerseDecompresser
from .constants import ASC_TO_EBC_DEF, ENDOFFILE, TREESIZE
from .header import TerseHeader


class PackDecompresser(TerseDecompresser):
    """Decompressor for TERSE files created with the PACK algorithm."""

    def __init__(
        self,
        in_stream: BinaryIO,
        out_stream: BinaryIO,
        header: TerseHeader,
    ) -> None:
        super().__init__(in_stream, out_stream, header)

    def decode(self) -> None:
        """Decompress a PACK-encoded TERSE stream.

        Implements the LZW-variant adaptive dictionary decompression
        exactly as in NonSpackDecompresser.java, including the LRU
        management via the Backward/Forward doubly-linked lists.
        """
        father: list[int] = [0] * TREESIZE
        char_ext: list[int] = [0] * TREESIZE
        backward: list[int] = [0] * TREESIZE
        forward: list[int] = [0] * TREESIZE

        # Initialise H2 / Father / CharExt
        # Java: H2 = 1 + AscToEbcDef[' ']
        h2: int = 1 + ASC_TO_EBC_DEF[ord(" ")]

        for h1 in range(258, 4096):
            father[h1] = h2
            char_ext[h1] = 1 + ASC_TO_EBC_DEF[ord(" ")]
            h2 = h1

        # Initialise doubly-linked LRU list (indices 258..4095)
        for h1 in range(258, 4095):
            backward[h1 + 1] = h1
            forward[h1] = h1 + 1

        backward[0] = 4095
        forward[0] = 258
        backward[258] = 0
        forward[4095] = 0

        x: int = 0
        d: int = self._input.get_blok()

        while d != ENDOFFILE:
            h: int = 0
            y: int = backward[0]
            q: int = backward[y]
            backward[0] = q
            forward[q] = 0
            h = y
            p: int = 0

            while d > 257:
                q = forward[d]
                r = backward[d]
                forward[r] = q
                backward[q] = r
                forward[d] = h
                backward[h] = d
                h = d
                e: int = father[d]
                father[d] = p
                p = d
                d = e

            q = forward[0]
            forward[y] = q
            backward[q] = y
            forward[0] = h
            backward[h] = 0
            char_ext[x] = d
            self.put_char(d)
            x = y

            while p != 0:
                e = father[p]
                self.put_char(char_ext[p])
                father[p] = d
                d = p
                p = e

            father[y] = d
            d = self._input.get_blok()
