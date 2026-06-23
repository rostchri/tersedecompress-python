"""
setup.py — Cython acceleration extension, graceful-optional.

``pip install .`` / ``pip wheel .`` attempts to compile
``src/tersedecompress/_terse_fast.pyx`` into a shared library.  If
compilation fails (missing C compiler, missing Python headers, …) the
build continues WITHOUT the extension and produces a pure-Python wheel.
The runtime fallback in ``tersedecompress.fast`` handles the absent
``.so`` transparently.

Package discovery, metadata and package_dir are declared entirely in
``pyproject.toml``; this file only wires in the optional Cython extension.

The separate ``setup_fast.py`` (for the opt-in
``python setup_fast.py build_ext --inplace`` workflow) is preserved
unchanged — both build paths remain functional.
"""

from __future__ import annotations

import os
import sys

from setuptools import setup
from setuptools.command.build_ext import build_ext as _build_ext


class OptionalBuildExt(_build_ext):
    """build_ext that swallows compiler errors instead of aborting the build.

    When a C compiler is absent or the Cython extension fails to compile,
    a warning is printed and the wheel is produced without the ``.so``.
    Importing ``tersedecompress`` will still work via the pure-Python
    fallback in ``tersedecompress.fast``.

    Set ``TERSEDECOMPRESS_REQUIRE_CYTHON=1`` to turn any failure into a
    hard error (useful in CI environments that must ship the accelerator).
    """

    def run(self) -> None:
        try:
            super().run()
        except Exception as exc:  # noqa: BLE001
            self._warn(f"build_ext.run() failed: {type(exc).__name__}: {exc}")

    def build_extension(self, ext) -> None:  # type: ignore[override]
        try:
            super().build_extension(ext)
        except Exception as exc:  # noqa: BLE001
            self._warn(
                f"Could not compile {ext.name!r}: "
                f"{type(exc).__name__}: {exc}. "
                "Installing without the Cython accelerator."
            )

    @staticmethod
    def _warn(msg: str) -> None:
        print(f"\n[tersedecompress] WARNING: {msg}\n", file=sys.stderr)
        if os.environ.get("TERSEDECOMPRESS_REQUIRE_CYTHON"):
            raise SystemExit(
                "[tersedecompress] TERSEDECOMPRESS_REQUIRE_CYTHON=1 — aborting."
            )


def _ext_modules() -> list:
    """Return Extension list, or [] when Cython is not importable."""
    try:
        from Cython.Build import cythonize
        from setuptools import Extension

        return cythonize(
            [
                Extension(
                    "tersedecompress._terse_fast",
                    sources=["src/tersedecompress/_terse_fast.pyx"],
                )
            ],
            compiler_directives={"language_level": "3"},
            nthreads=1,
        )
    except Exception as exc:  # noqa: BLE001
        print(
            f"\n[tersedecompress] WARNING: Cython unavailable "
            f"({type(exc).__name__}: {exc}). "
            "Building without the accelerator extension.\n",
            file=sys.stderr,
        )
        return []


setup(
    ext_modules=_ext_modules(),
    cmdclass={"build_ext": OptionalBuildExt},
)
