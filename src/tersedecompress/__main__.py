"""
CLI entry point for tersedecompress.

Usage:
    python -m tersedecompress <input> <output> [-b] [-t]
    python -m tersedecompress <input> [-t]        # output defaults to <input>.txt
    python -m tersedecompress <input> -            # stdout
    python -m tersedecompress - <output>           # stdin
    python -m tersedecompress - -                  # stdin → stdout
    python -m tersedecompress --pipe               # stdin → stdout (shorthand)

Options:
    -b    Binary mode (no EBCDIC → ASCII conversion)
    -t    Text mode (EBCDIC → ASCII + newlines)  [default]
    -h    Show help

When neither -b nor -t is specified, text mode is the default,
matching the behaviour of the original Java CLI.

Use '-' as input or output path to read from stdin / write to stdout.
--pipe is a shorthand for '- -'.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from . import __version__
from .core import decompress_file

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

_STDIO_SENTINEL = "-"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tersedecompress",
        description=(
            "Decompress a file compressed using the terse program on z/OS.\n"
            "Default mode is text mode (EBCDIC → ASCII conversion).\n"
            "If no output file is provided, defaults to <input>.txt\n"
            "Use '-' for stdin (input) or stdout (output).\n"
            "--pipe is a shorthand for '- -'."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "input",
        nargs="?",
        default=None,
        help="Input tersed file path, or '-' for stdin",
    )
    parser.add_argument(
        "output",
        nargs="?",
        default=None,
        help="Output file path, or '-' for stdout (default: <input>.txt in text mode)",
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
        "--pipe",
        action="store_true",
        default=False,
        help="Pipe mode: read from stdin and write to stdout (shorthand for '- -')",
    )
    parser.add_argument(
        "--max-output-bytes",
        type=int,
        default=None,
        metavar="BYTES",
        help="Abort with an error if the decompressed output exceeds BYTES bytes",
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
    max_output_bytes: int | None = args.max_output_bytes

    # Resolve pipe mode: --pipe or missing positional args when piping
    pipe_mode = args.pipe
    if pipe_mode:
        if args.input is not None and args.input != _STDIO_SENTINEL:
            logger.error("--pipe cannot be combined with an explicit input path")
            return 1
        if args.output is not None and args.output != _STDIO_SENTINEL:
            logger.error("--pipe cannot be combined with an explicit output path")
            return 1
        input_arg = _STDIO_SENTINEL
        output_arg = _STDIO_SENTINEL
    else:
        if args.input is None:
            logger.error("input file required (or use --pipe for stdin)")
            return 1
        input_arg = args.input
        output_arg = args.output

    use_stdin = input_arg == _STDIO_SENTINEL
    use_stdout = output_arg == _STDIO_SENTINEL

    # When writing to stdout, suppress INFO logs to avoid polluting the stream
    if use_stdout:
        logging.getLogger().setLevel(logging.WARNING)

    # Resolve output when writing to a file
    if not use_stdout:
        if output_arg is not None:
            output_path: Path | None = Path(output_arg)
        elif text_mode:
            if use_stdin:
                raise ValueError("stdin + auto output only supported with an explicit output path (-o)")
            output_path = Path(input_arg).with_suffix(Path(input_arg).suffix + ".txt")
        else:
            logger.error("output file required in binary mode (-b)")
            return 1
    else:
        output_path = None

    if not use_stdin:
        input_path = Path(input_arg)
        if not input_path.exists():
            logger.error("input file not found: %s", input_path)
            return 1
        logger.info(
            "Attempting to decompress input file (%s) to output file (%s)",
            input_path,
            output_path if output_path is not None else "<stdout>",
        )
    else:
        logger.info(
            "Attempting to decompress from stdin to %s",
            output_path if output_path is not None else "<stdout>",
        )

    try:
        if use_stdin and use_stdout:
            _stream_to_stream(sys.stdin.buffer, sys.stdout.buffer, text_mode, max_output_bytes)
        elif use_stdin:
            if output_path is None:
                raise ValueError("output_path must not be None for stream-to-file mode")
            _stream_to_file(sys.stdin.buffer, output_path, text_mode, max_output_bytes)
        elif use_stdout:
            _file_to_stream(Path(input_arg), sys.stdout.buffer, text_mode, max_output_bytes)
        else:
            if output_path is None:
                raise ValueError("output_path must not be None for file-to-file mode")
            decompress_file(Path(input_arg), output_path, text_mode=text_mode, max_output_bytes=max_output_bytes)
    except Exception as exc:  # noqa: BLE001
        logger.error("Something went wrong, Exception %s", exc)
        return 1

    logger.info("Processing completed")
    return 0


def _stream_to_stream(
    in_stream: "BinaryIO",
    out_stream: "BinaryIO",
    text_mode: bool,
    max_output_bytes: int | None = None,
) -> None:
    """Decompress from an input stream to an output stream."""
    from .base import TerseDecompresser

    with TerseDecompresser.create(
        in_stream, out_stream, text_mode=text_mode, max_output_bytes=max_output_bytes
    ) as d:
        d.decode()


def _stream_to_file(
    in_stream: "BinaryIO",
    output_path: Path,
    text_mode: bool,
    max_output_bytes: int | None = None,
) -> None:
    """Decompress from a binary input stream, writing result to a file."""
    import builtins as _builtins

    from .base import TerseDecompresser

    with _builtins.open(output_path, "wb") as out_f:
        with TerseDecompresser.create(
            in_stream, out_f, text_mode=text_mode, max_output_bytes=max_output_bytes
        ) as d:
            d.decode()


def _file_to_stream(
    input_path: Path,
    out_stream: "BinaryIO",
    text_mode: bool,
    max_output_bytes: int | None = None,
) -> None:
    """Decompress a file, writing result to a binary output stream."""
    import builtins as _builtins

    from .base import TerseDecompresser

    with _builtins.open(input_path, "rb") as in_f:
        with TerseDecompresser.create(
            in_f, out_stream, text_mode=text_mode, max_output_bytes=max_output_bytes
        ) as d:
            d.decode()


from typing import BinaryIO  # noqa: E402 — used in annotations above


if __name__ == "__main__":
    sys.exit(main())
