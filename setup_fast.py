"""
Standalone build script for the OPTIONAL Cython acceleration extension.

This is intentionally separate from ``pyproject.toml`` / the normal package
build: ``pip install tersedecompress`` stays pure-Python and needs no compiler.
Building the accelerator is an explicit, opt-in step:

    pip install cython
    python setup_fast.py build_ext --inplace

That compiles ``src/tersedecompress/_terse_fast.pyx`` into a shared library
placed next to the package sources, e.g.::

    src/tersedecompress/_terse_fast.cpython-311-x86_64-linux-gnu.so

Once that file sits beside the installed package, ``tersedecompress`` picks it
up automatically (see ``tersedecompress.fast``); no source changes are needed.
The produced ``.so`` is ABI-specific — build it against the same CPython
version/platform you run on.
"""

from setuptools import Extension, setup
from Cython.Build import cythonize

setup(
    name="tersedecompress-fast",
    package_dir={"": "src"},
    ext_modules=cythonize(
        [
            Extension(
                "tersedecompress._terse_fast",
                ["src/tersedecompress/_terse_fast.pyx"],
            )
        ],
        compiler_directives={"language_level": "3"},
    ),
)
