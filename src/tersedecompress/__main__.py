"""
CLI entry point for tersedecompress.

Usage:
    python -m tersedecompress <input> <output> [-b] [-t]
    python -m tersedecompress <input> [-t]        # output defaults to <input>.txt

Options:
    -b    Binary mode (no EBCDIC → ASCII conversion)
    -t    Text mode (EBCDIC → ASCII + newlines)  [default]
    -h    Show help

When neither -b nor -t is specified, text mode is the default,
matching the behaviour of the original Java CLI.
"""

import argparse
import sys
from pathlib import Path

from . import __version__
from .core import decompress_file


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tersedecompress",
        description=(
            "Decompress a file compressed using the terse program on z/OS.\n"
            "Default mode is text mode (EBCDIC → ASCII conversion).\n"
            "If no output file is provided, defaults to <input>.txt"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("input", help="Input tersed file path")
    parser.add_argument(
        "output",
        nargs="?",
        default=None,
        help="Output file path (default: <input>.txt in text mode)",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "-b",
        "--binary",
        action="store_true",
        help="Binary mode — no EBCDIC → ASCII conversion",
    )
    mode.add_argument(
        "-t",
        "--text",
        action="store_true",
        default=True,
        help="Text mode — EBCDIC → ASCII conversion (default)",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    text_mode: bool = not args.binary

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: input file not found: {input_path}", file=sys.stderr)
        return 1

    output_path: Path
    if args.output is not None:
        output_path = Path(args.output)
    elif text_mode:
        output_path = input_path.with_suffix(input_path.suffix + ".txt")
    else:
        print(
            "Error: output file required in binary mode (-b)", file=sys.stderr
        )
        return 1

    print(
        f"Attempting to decompress input file ({input_path}) "
        f"to output file ({output_path})"
    )

    try:
        decompress_file(input_path, output_path, text_mode=text_mode)
    except Exception as exc:  # noqa: BLE001
        print(f"Something went wrong, Exception {exc}", file=sys.stderr)
        return 1

    print("Processing completed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
