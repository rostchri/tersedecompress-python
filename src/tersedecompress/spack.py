"""
SPACK decompressor (adaptive Huffman tree + LRU used by IBM z/OS TERSE).

Ported exactly from SpackDecompresser.java.

The TreeRecord fields have a dual semantics for NextCount:
  - While a node is in the LRU free list: NextCount is the forward pointer
    to the next free node (positive index or NONE).
  - Once a node is in active use: NextCount is a negative reference count
    (-1 means referenced once, -2 twice, etc.).
  - Leaf nodes (index 0..CODESIZE) start with NextCount = NONE (-1)
    because they are permanently referenced.

LruAdd inserts a node at the *back* of the LRU queue (most-recently-used).
LruKill removes the node at the *front* (least-recently-used).
"""

from typing import BinaryIO

from .base import TerseDecompresser
from .constants import BASE, CODESIZE, ENDOFFILE, NONE, STACKSIZE, TREESIZE
from .header import TerseHeader


class _TreeRecord:
    """Single node in the SPACK decompression tree."""

    __slots__ = ("left", "right", "back", "next_count")

    def __init__(self) -> None:
        self.left: int = 0
        self.right: int = 0
        self.back: int = 0
        self.next_count: int = 0


class SpackDecompresser(TerseDecompresser):
    """Decompressor for TERSE files created with the SPACK algorithm."""

    def __init__(
        self,
        in_stream: BinaryIO,
        out_stream: BinaryIO,
        header: TerseHeader,
    ) -> None:
        super().__init__(in_stream, out_stream, header)

        self._node: int = 0
        self._tree_avail: int = 0
        self._tree: list[_TreeRecord] = [_TreeRecord() for _ in range(TREESIZE + 1)]
        self._stack_head: int = 0
        self._stack_data: list[int] = [0] * (STACKSIZE + 1)

        # Instance variables for BumpRef (mirrors Java field declarations)
        self._forwards: int = 0
        self._prev: int = 0

        # Instance variables for LruKill
        self._lru_p: int = 0
        self._lru_q: int = 0
        self._lru_r: int = 0

        # Instance variable for LruAdd
        self._lru_back: int = 0

    # ------------------------------------------------------------------
    # Tree management
    # ------------------------------------------------------------------

    def _tree_init(self) -> None:
        """Initialise the adaptive tree to the start-of-stream state."""
        tree = self._tree

        init_index = BASE
        while init_index <= CODESIZE:
            tree[init_index].left = NONE
            tree[init_index].right = init_index
            init_index += 1

        for init_index in range(CODESIZE + 1, TREESIZE):
            tree[init_index].next_count = init_index + 1
            tree[init_index].left = NONE
            tree[init_index].right = NONE

        tree[TREESIZE].next_count = NONE
        tree[BASE].next_count = BASE
        tree[BASE].back = BASE

        for init_index in range(1, CODESIZE + 1):
            tree[init_index].next_count = NONE

        self._tree_avail = CODESIZE + 1

    def _get_tree_node(self) -> int:
        """Remove and return the first node from the free list."""
        node = self._tree_avail
        self._tree_avail = self._tree[node].next_count
        self._node = node
        return node

    def _bump_ref(self, bref: int) -> None:
        """Increment the reference count for node *bref*.

        If the node was in the LRU list (positive NextCount), remove it
        and set its reference count to -1.
        If it was already referenced (negative NextCount), decrement further.
        """
        tree = self._tree
        if tree[bref].next_count < 0:
            tree[bref].next_count -= 1
        else:
            self._forwards = tree[bref].next_count
            self._prev = tree[bref].back
            tree[self._prev].next_count = self._forwards
            tree[self._forwards].back = self._prev
            tree[bref].next_count = -1

    def _lru_kill(self) -> None:
        """Evict the least-recently-used node from the tree."""
        tree = self._tree
        self._lru_p = tree[0].next_count
        self._lru_q = tree[self._lru_p].next_count
        self._lru_r = tree[self._lru_p].back
        tree[self._lru_q].back = self._lru_r
        tree[self._lru_r].next_count = self._lru_q
        self._delete_ref(tree[self._lru_p].left)
        self._delete_ref(tree[self._lru_p].right)
        tree[self._lru_p].next_count = self._tree_avail
        self._tree_avail = self._lru_p

    def _delete_ref(self, dref: int) -> None:
        """Decrement the reference count, and re-add to LRU if it drops to zero."""
        tree = self._tree
        if tree[dref].next_count == -1:
            self._lru_add(dref)
        else:
            tree[dref].next_count += 1

    def _lru_add(self, lru_next: int) -> None:
        """Append *lru_next* to the back (most-recently-used) of the LRU list."""
        tree = self._tree
        self._lru_back = tree[BASE].back
        tree[lru_next].next_count = BASE
        tree[BASE].back = lru_next
        tree[lru_next].back = self._lru_back
        tree[self._lru_back].next_count = lru_next

    # ------------------------------------------------------------------
    # Output helper
    # ------------------------------------------------------------------

    def _put_chars(self, x: int) -> None:
        """Recursively expand node *x* and emit all leaf characters.

        Uses an explicit stack (self._stack_data) to avoid Python
        recursion-depth limits for large compressed sequences.

        Args:
            x: Tree node index to expand.

        Raises:
            IOError: If a negative node index is encountered (corrupt data).
        """
        stack_data = self._stack_data
        self._stack_head = 0

        while True:
            while x > CODESIZE:
                self._stack_head += 1
                stack_data[self._stack_head] = self._tree[x].right
                x = self._tree[x].left

            if x < 0:
                raise IOError(
                    "Unexpected sequence, seems like file is corrupted"
                )

            self.put_char(x)

            if self._stack_head > 0:
                x = stack_data[self._stack_head]
                self._stack_head -= 1
            else:
                break

    # ------------------------------------------------------------------
    # Main decode
    # ------------------------------------------------------------------

    def decode(self) -> None:
        """Decompress a SPACK-encoded TERSE stream.

        Implements the adaptive Huffman + LRU decompression algorithm
        exactly as in SpackDecompresser.java.
        """
        self._tree_avail = 0
        self._tree_init()
        self._tree[TREESIZE - 1].next_count = NONE

        h: int = self._input.get_blok()

        if h != ENDOFFILE:
            self._put_chars(h)
            g: int = self._input.get_blok()

            while g != ENDOFFILE:
                if self._tree_avail == NONE:
                    self._lru_kill()

                self._put_chars(g)
                n: int = self._get_tree_node()
                self._tree[n].left = h
                self._tree[n].right = g
                self._bump_ref(h)
                self._bump_ref(g)
                self._lru_add(n)
                h = g
                g = self._input.get_blok()
