# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True
"""
Cython acceleration core for the TERSE PACK and SPACK decoders.

This module is an OPTIONAL drop-in accelerator.  It mirrors
``NonSpackDecompresser.decode()`` / ``SpackDecompresser.decode()`` together with
``TerseDecompresser.put_char`` / ``end_record`` from the pure-Python
implementation EXACTLY, but replaces:

  * the four parallel Python lists (Father/CharExt/Backward/Forward) with
    C ``int`` arrays,
  * the per-byte ``get_blok()`` method call with an inline 12-bit reader over
    a contiguous ``const unsigned char[::1]`` buffer,
  * the per-byte ``bytearray.append`` + per-record method dispatch with a
    self-growing C output buffer.

Output is streamed to the supplied ``out_stream`` in chunks (default 1 MiB)
so that very large decompressed results never have to be held in RAM in one
piece — this keeps the SpooledTemporaryFile behaviour of ``TerseFile`` intact.

Both PACK (``decode_pack``) and SPACK (``decode_spack``) are handled here;
``decode_spack`` mirrors ``SpackDecompresser`` (adaptive Huffman tree + LRU)
exactly, with the per-node ``_TreeRecord`` objects replaced by four parallel
C ``int`` arrays and an explicit C stack.  Any environment without the compiled
extension falls back to the pure-Python decoder transparently (see
``tersedecompress.fast``).
"""

from libc.stdlib cimport malloc, free, realloc
from libc.string cimport memcpy
from cpython.bytes cimport PyBytes_FromStringAndSize

DEF TREESIZE = 4096
DEF ENDOFFILE = 0
DEF RECORDMARK = 257
DEF DEFAULT_FLUSH = 1 << 20    # 1 MiB

# SPACK-specific constants (mirror tersedecompress.constants)
DEF BASE = 0
DEF CODESIZE = 257
DEF NONE = -1
DEF STACKSIZE = 0x07FF         # 2k - 1


cdef class _Buf:
    """Plain growable C byte buffer — used for the current record (no stream)."""
    cdef unsigned char* buf
    cdef Py_ssize_t cap
    cdef Py_ssize_t length

    def __cinit__(self, Py_ssize_t hint):
        if hint < 64:
            hint = 64
        self.buf = <unsigned char*>malloc(hint)
        if self.buf == NULL:
            raise MemoryError()
        self.cap = hint
        self.length = 0

    def __dealloc__(self):
        if self.buf != NULL:
            free(self.buf)
            self.buf = NULL

    cdef inline int push(self, unsigned char b) except -1:
        cdef Py_ssize_t newcap
        cdef unsigned char* nb
        if self.length + 1 > self.cap:
            newcap = self.cap * 2
            nb = <unsigned char*>realloc(self.buf, newcap)
            if nb == NULL:
                raise MemoryError()
            self.buf = nb
            self.cap = newcap
        self.buf[self.length] = b
        self.length += 1
        return 0


cdef class _Out:
    """Growable C byte buffer that flushes to a Python stream in chunks."""
    cdef unsigned char* buf
    cdef Py_ssize_t cap
    cdef Py_ssize_t length
    cdef object stream
    cdef Py_ssize_t threshold
    cdef Py_ssize_t total       # bytes already flushed to the stream

    def __cinit__(self, object stream, Py_ssize_t threshold):
        if threshold < 65536:
            threshold = 65536
        self.cap = threshold + 65536
        self.buf = <unsigned char*>malloc(self.cap)
        if self.buf == NULL:
            raise MemoryError()
        self.length = 0
        self.stream = stream
        self.threshold = threshold
        self.total = 0

    def __dealloc__(self):
        if self.buf != NULL:
            free(self.buf)
            self.buf = NULL

    cdef inline int _grow(self, Py_ssize_t need) except -1:
        cdef Py_ssize_t newcap = self.cap
        cdef unsigned char* nb
        while newcap < self.length + need:
            newcap = newcap * 2
        nb = <unsigned char*>realloc(self.buf, newcap)
        if nb == NULL:
            raise MemoryError()
        self.buf = nb
        self.cap = newcap
        return 0

    cdef int flush(self) except -1:
        if self.length:
            self.stream.write(PyBytes_FromStringAndSize(<char*>self.buf, self.length))
            self.total += self.length
            self.length = 0
        return 0

    cdef inline int push(self, unsigned char b) except -1:
        if self.length + 1 > self.cap:
            self._grow(1)
        self.buf[self.length] = b
        self.length += 1
        if self.length >= self.threshold:
            self.flush()
        return 0

    cdef inline int extend(self, const unsigned char* src, Py_ssize_t n) except -1:
        if self.length + n > self.cap:
            self._grow(n)
        memcpy(self.buf + self.length, src, n)
        self.length += n
        if self.length >= self.threshold:
            self.flush()
        return 0


cdef inline int _check_limit(_Out out, Py_ssize_t add, Py_ssize_t limit) except -1:
    if limit >= 0 and out.total + out.length + add > limit:
        raise IOError("Decompression bomb protection: output exceeds limit")
    return 0


cdef inline int _end_record(_Out out, _Buf rec, bint text_flag,
                            bint variable_flag, Py_ssize_t limit) except -1:
    cdef unsigned int rdw
    cdef unsigned char rb[4]
    if variable_flag and not text_flag:
        rdw = (<unsigned int>(rec.length + 4) << 16) & 0xFFFFFFFF
        rb[0] = <unsigned char>((rdw >> 24) & 0xFF)
        rb[1] = <unsigned char>((rdw >> 16) & 0xFF)
        rb[2] = <unsigned char>((rdw >> 8) & 0xFF)
        rb[3] = <unsigned char>(rdw & 0xFF)
        _check_limit(out, 4, limit)
        out.extend(rb, 4)
    _check_limit(out, rec.length, limit)
    out.extend(rec.buf, rec.length)
    rec.length = 0
    if text_flag:
        _check_limit(out, 1, limit)
        out.push(<unsigned char>10)   # b"\n"
    return 0


cdef inline int _put_char(int xx, _Out out, _Buf rec, int record_length,
                          bint host_flag, bint text_flag, bint variable_flag,
                          const unsigned char[::1] ebc_to_asc,
                          Py_ssize_t limit) except -1:
    cdef int byte_val
    if xx == ENDOFFILE:
        if host_flag and text_flag and variable_flag:
            _end_record(out, rec, text_flag, variable_flag, limit)
        return 0
    if xx == RECORDMARK:
        if variable_flag:
            _end_record(out, rec, text_flag, variable_flag, limit)
        return 0
    byte_val = (xx - 1) & 0xFF
    if host_flag and text_flag:
        byte_val = ebc_to_asc[byte_val]
    rec.push(<unsigned char>byte_val)
    if (not variable_flag) and rec.length >= record_length:
        _end_record(out, rec, text_flag, variable_flag, limit)
    return 0


cdef inline int _get_blok(const unsigned char[::1] payload, Py_ssize_t n,
                          Py_ssize_t* bp, int* phase, int* saved) except -2:
    cdef int b1, b2, b3
    if phase[0] == 0:
        if bp[0] >= n:
            return ENDOFFILE
        b1 = payload[bp[0]]; bp[0] += 1
        if bp[0] >= n:
            raise IOError("Tried to read 12 bits but found EOF after 8 bits.")
        b2 = payload[bp[0]]; bp[0] += 1
        saved[0] = b2 & 0x0F
        phase[0] = 1
        return (b1 << 4) | (b2 >> 4)
    else:
        if bp[0] >= n:
            return ENDOFFILE
        b3 = payload[bp[0]]; bp[0] += 1
        phase[0] = 0
        return (saved[0] << 8) | b3


def decode_pack(const unsigned char[::1] payload not None,
                int record_length,
                bint host_flag,
                bint text_flag,
                bint variable_flag,
                int space_code,
                const unsigned char[::1] ebc_to_asc not None,
                object out_stream,
                max_output_bytes=None,
                Py_ssize_t flush_threshold=DEFAULT_FLUSH):
    """Decode a PACK payload (header already stripped) into *out_stream*.

    The decompressed bytes are written to ``out_stream.write`` in chunks; the
    function returns the total number of decompressed bytes written.
    """
    cdef Py_ssize_t n = payload.shape[0]

    cdef int* father = <int*>malloc(TREESIZE * sizeof(int))
    cdef int* char_ext = <int*>malloc(TREESIZE * sizeof(int))
    cdef int* backward = <int*>malloc(TREESIZE * sizeof(int))
    cdef int* forward = <int*>malloc(TREESIZE * sizeof(int))
    if not father or not char_ext or not backward or not forward:
        free(father); free(char_ext); free(backward); free(forward)
        raise MemoryError()

    cdef _Out out = _Out.__new__(_Out, out_stream, flush_threshold)
    cdef _Buf rec = _Buf.__new__(
        _Buf, record_length + 64 if record_length > 0 else 4096)

    cdef Py_ssize_t limit = -1
    if max_output_bytes is not None:
        limit = <Py_ssize_t>max_output_bytes

    cdef int h1, h2
    cdef int x, y, d, h, p, q, r, e
    cdef Py_ssize_t bp = 0
    cdef int phase = 0
    cdef int saved = 0
    cdef int iter_count
    cdef Py_ssize_t written

    try:
        h2 = space_code
        for h1 in range(258, TREESIZE):
            father[h1] = h2
            char_ext[h1] = space_code
            h2 = h1
        for h1 in range(258, TREESIZE - 1):
            backward[h1 + 1] = h1
            forward[h1] = h1 + 1
        backward[0] = TREESIZE - 1
        forward[0] = 258
        backward[258] = 0
        forward[TREESIZE - 1] = 0

        x = 0
        d = _get_blok(payload, n, &bp, &phase, &saved)

        while d != ENDOFFILE:
            y = backward[0]
            q = backward[y]
            backward[0] = q
            forward[q] = 0
            h = y
            p = 0
            iter_count = 0
            while d > 257:
                iter_count += 1
                if iter_count > TREESIZE:
                    raise IOError("PACK decode: cycle in father[] (corrupt input)")
                q = forward[d]
                r = backward[d]
                forward[r] = q
                backward[q] = r
                forward[d] = h
                backward[h] = d
                h = d
                e = father[d]
                father[d] = p
                p = d
                d = e
            q = forward[0]
            forward[y] = q
            backward[q] = y
            forward[0] = h
            backward[h] = 0
            char_ext[x] = d
            _put_char(d, out, rec, record_length, host_flag, text_flag,
                      variable_flag, ebc_to_asc, limit)
            x = y
            while p != 0:
                e = father[p]
                _put_char(char_ext[p], out, rec, record_length, host_flag,
                          text_flag, variable_flag, ebc_to_asc, limit)
                father[p] = d
                d = p
                p = e
            father[y] = d
            d = _get_blok(payload, n, &bp, &phase, &saved)

        # close(): flush trailing record exactly like the pure-Python decoder
        if rec.length > 0 or (text_flag and variable_flag):
            _end_record(out, rec, text_flag, variable_flag, limit)

        written = out.total + out.length
        out.flush()
        return written
    finally:
        free(father); free(char_ext); free(backward); free(forward)


# =============================================================================
# SPACK (adaptive Huffman tree + LRU) — mirror of SpackDecompresser
# =============================================================================
# The Python _TreeRecord(left, right, back, next_count) becomes four parallel
# C int arrays of size TREESIZE+1.  NextCount has dual semantics: positive =
# free-list forward pointer / LRU forward link; negative = reference count.

cdef inline int _sp_lru_add(int* next_count, int* back, int lru_next):
    cdef int lru_back = back[BASE]
    next_count[lru_next] = BASE
    back[BASE] = lru_next
    back[lru_next] = lru_back
    next_count[lru_back] = lru_next
    return 0


cdef inline int _sp_delete_ref(int* next_count, int* back, int dref):
    if next_count[dref] == -1:
        _sp_lru_add(next_count, back, dref)
    else:
        next_count[dref] += 1
    return 0


cdef inline int _sp_bump_ref(int* next_count, int* back, int bref):
    cdef int forwards, prev
    if next_count[bref] < 0:
        next_count[bref] -= 1
    else:
        forwards = next_count[bref]
        prev = back[bref]
        next_count[prev] = forwards
        back[forwards] = prev
        next_count[bref] = -1
    return 0


cdef inline int _sp_lru_kill(int* left, int* right, int* back, int* next_count,
                             int* tree_avail):
    cdef int lru_p = next_count[0]
    cdef int lru_q = next_count[lru_p]
    cdef int lru_r = back[lru_p]
    back[lru_q] = lru_r
    next_count[lru_r] = lru_q
    _sp_delete_ref(next_count, back, left[lru_p])
    _sp_delete_ref(next_count, back, right[lru_p])
    next_count[lru_p] = tree_avail[0]
    tree_avail[0] = lru_p
    return 0


cdef inline int _sp_get_tree_node(int* next_count, int* tree_avail) except -1:
    cdef int node = tree_avail[0]
    if not (0 <= node < TREESIZE):
        raise IOError("SPACK tree node index out of range (corrupt input)")
    tree_avail[0] = next_count[node]
    return node


cdef inline int _sp_put_chars(int x, int* left, int* right, int* stack_data,
                              _Out out, _Buf rec, int record_length,
                              bint host_flag, bint text_flag, bint variable_flag,
                              const unsigned char[::1] ebc_to_asc,
                              Py_ssize_t limit) except -1:
    cdef int stack_head = 0
    while True:
        while x > CODESIZE:
            stack_head += 1
            if stack_head > STACKSIZE:
                raise IOError("SPACK stack overflow: corrupt input")
            stack_data[stack_head] = right[x]
            x = left[x]
        if x < 0:
            raise IOError("Unexpected sequence, seems like file is corrupted")
        _put_char(x, out, rec, record_length, host_flag, text_flag,
                  variable_flag, ebc_to_asc, limit)
        if stack_head > 0:
            x = stack_data[stack_head]
            stack_head -= 1
        else:
            break
    return 0


def decode_spack(const unsigned char[::1] payload not None,
                 int record_length,
                 bint host_flag,
                 bint text_flag,
                 bint variable_flag,
                 int space_code,
                 const unsigned char[::1] ebc_to_asc not None,
                 object out_stream,
                 max_output_bytes=None,
                 Py_ssize_t flush_threshold=DEFAULT_FLUSH):
    """Decode a SPACK payload (header already stripped) into *out_stream*.

    ``space_code`` is accepted for signature symmetry with ``decode_pack`` but
    is unused by the SPACK algorithm.  Returns the number of bytes written.
    """
    cdef Py_ssize_t n = payload.shape[0]
    cdef Py_ssize_t nslots = TREESIZE + 1

    cdef int* left = <int*>malloc(nslots * sizeof(int))
    cdef int* right = <int*>malloc(nslots * sizeof(int))
    cdef int* back = <int*>malloc(nslots * sizeof(int))
    cdef int* next_count = <int*>malloc(nslots * sizeof(int))
    cdef int* stack_data = <int*>malloc((STACKSIZE + 1) * sizeof(int))
    if not left or not right or not back or not next_count or not stack_data:
        free(left); free(right); free(back); free(next_count); free(stack_data)
        raise MemoryError()

    cdef _Out out = _Out.__new__(_Out, out_stream, flush_threshold)
    cdef _Buf rec = _Buf.__new__(
        _Buf, record_length + 64 if record_length > 0 else 4096)

    cdef Py_ssize_t limit = -1
    if max_output_bytes is not None:
        limit = <Py_ssize_t>max_output_bytes

    cdef Py_ssize_t bp = 0
    cdef int phase = 0
    cdef int saved = 0
    cdef int tree_avail
    cdef int i, h, g, node
    cdef Py_ssize_t written

    try:
        # --- tree_init (mirror SpackDecompresser._tree_init) ---
        for i in range(BASE, CODESIZE + 1):        # 0..257
            left[i] = NONE
            right[i] = i
        for i in range(CODESIZE + 1, TREESIZE):    # 258..4095
            next_count[i] = i + 1
            left[i] = NONE
            right[i] = NONE
        next_count[TREESIZE] = NONE
        next_count[BASE] = BASE
        back[BASE] = BASE
        for i in range(1, CODESIZE + 1):           # 1..257
            next_count[i] = NONE
        tree_avail = CODESIZE + 1
        # decode() applies this extra fix-up after _tree_init:
        next_count[TREESIZE - 1] = NONE

        h = _get_blok(payload, n, &bp, &phase, &saved)
        if h != ENDOFFILE:
            _sp_put_chars(h, left, right, stack_data, out, rec, record_length,
                          host_flag, text_flag, variable_flag, ebc_to_asc, limit)
            g = _get_blok(payload, n, &bp, &phase, &saved)
            while g != ENDOFFILE:
                if tree_avail == NONE:
                    _sp_lru_kill(left, right, back, next_count, &tree_avail)
                _sp_put_chars(g, left, right, stack_data, out, rec,
                              record_length, host_flag, text_flag,
                              variable_flag, ebc_to_asc, limit)
                node = _sp_get_tree_node(next_count, &tree_avail)
                left[node] = h
                right[node] = g
                _sp_bump_ref(next_count, back, h)
                _sp_bump_ref(next_count, back, g)
                _sp_lru_add(next_count, back, node)
                h = g
                g = _get_blok(payload, n, &bp, &phase, &saved)

        if rec.length > 0 or (text_flag and variable_flag):
            _end_record(out, rec, text_flag, variable_flag, limit)

        written = out.total + out.length
        out.flush()
        return written
    finally:
        free(left); free(right); free(back); free(next_count); free(stack_data)
