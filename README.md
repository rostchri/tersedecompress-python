# tersedecompress-python

Python 3.11+ port of the IBM z/OS TERSE decompression library
([openmainframeproject/tersedecompress](https://github.com/openmainframeproject/tersedecompress)).

Supports both compression algorithms used by the `TERSE`/`AMATERSE` utility on z/OS:

- **PACK** — LZW-variant with adaptive dictionary (header byte `0x02`)
- **SPACK** — Adaptive Huffman tree with LRU eviction (header byte `0x05`)

Both fixed-length (FB) and variable-length (VB) record formats are supported,
in binary mode (raw bytes) and text mode (EBCDIC → ASCII with newline conversion).

## Installation

```bash
pip install tersedecompress
```

## Usage

### Command line

```bash
# Text mode (EBCDIC -> ASCII, default)
python -m tersedecompress input.tersed output.txt

# Binary mode (raw bytes, no conversion)
python -m tersedecompress input.tersed output.bin -b
```

### Python API

```python
from tersedecompress import decompress, decompress_file

# Decompress bytes to bytes (binary mode)
data = open("input.tersed", "rb").read()
output = decompress(data)

# Decompress with EBCDIC -> ASCII conversion
output = decompress(data, text_mode=True)

# Decompress a file
decompress_file("input.tersed", "output.txt", text_mode=True)
```

## Implementation notes

- The EBCDIC → ASCII conversion table (`EbcToAscAlmcopy`) is taken verbatim
  from the Java source. It differs subtly from `codecs.cp037`.
- The PACK decoder uses four parallel integer arrays (Father, CharExt,
  Backward, Forward) with a doubly-linked LRU list.
- The SPACK decoder's `TreeRecord.NextCount` field has dual semantics:
  positive values are free-list forward pointers; negative values are
  reference counts (−1 = one reference, −2 = two, …).
- No external dependencies — only Python standard library.

## Optional C acceleration (Cython)

The decoder is inherently byte-at-a-time: for every output byte the pure-Python
hot loop makes a function call (`get_blok` + `put_char`) and chases pointers
through four parallel lists. Measured throughput is therefore only
**~0.6 MB/s**, and a 1 GB compressed file would take well over an hour. Profiling
confirmed the cost is the per-byte interpreter overhead, not the algorithm:
careful pure-Python tuning (reading the whole input at once, pre-unpacking the
12-bit codes, inlining) tops out at ~1.6× — it does not change the order of
magnitude. The work is also strictly sequential (the adaptive dictionary
depends on every preceding code), so it cannot be parallelised.

Those facts motivated an **optional** Cython core for the PACK algorithm
(`src/tersedecompress/_terse_fast.pyx`): the four dictionaries become C `int`
arrays, the 12-bit reader is inlined over a contiguous buffer, and output is
streamed to the target stream from a C buffer. It is **byte-for-byte identical**
to the pure-Python decoder (binary/text, FB/VB) and runs **~90–95× faster
(~55 MB/s)** — a 1 GB file in well under a minute.

The accelerator is entirely opt-in and self-contained:

- `pip install tersedecompress` stays **pure-Python** and needs no compiler.
- If the compiled extension is *not* present — or the input uses **SPACK**,
  which the C core does not implement — the library falls back to the
  pure-Python decoder automatically. Same API, same output.

### Building and using the `.so`

```bash
pip install cython
python setup_fast.py build_ext --inplace
```

This compiles the extension into a shared library next to the package sources,
e.g. `src/tersedecompress/_terse_fast.cpython-311-x86_64-linux-gnu.so`. **Using
it requires no code changes whatsoever:** as long as that `.so` sits beside the
installed `tersedecompress` package, it is imported automatically and every
`decompress()`, `decompress_file()` and `tersedecompress.open()` call on a PACK
stream transparently uses the fast path. To confirm it is active:

```python
import tersedecompress.fast as fast
print(fast.is_available())   # True when the .so is loaded
```

The `.so` is ABI-specific — build it against the same CPython version and
platform you run on (the generated `.so` and intermediate `.c` are gitignored;
only the `.pyx` source is tracked).

## Testing

```bash
pip install -e ".[test]"
TEST_DATA_DIR=/path/to/tersedecompress-testdata pytest tests/ -v
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).

Original Java implementation: Copyright 2018 IBM Corp., Iain Lewis, Klaus Egeler,
Boris Barth, Andrew Rowley, Mario Bezzi.
