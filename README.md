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

## Testing

```bash
pip install -e ".[test]"
TEST_DATA_DIR=/path/to/tersedecompress-testdata pytest tests/ -v
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).

Original Java implementation: Copyright 2018 IBM Corp., Iain Lewis, Klaus Egeler,
Boris Barth, Andrew Rowley, Mario Bezzi.
