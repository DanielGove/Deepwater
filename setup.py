"""
Setup script for Deepwater with Cython extensions.
Compiles hot paths to C for maximum performance.
"""
from setuptools import setup, Extension
from Cython.Build import cythonize
import sys

# Compiler optimization flags
extra_compile_args = [
    "-O3",              # Maximum optimization
    "-march=native",    # Use CPU-specific instructions
    "-ffast-math",      # Aggressive floating-point optimizations
]

# Platform-specific adjustments
if sys.platform == "darwin":
    extra_compile_args.remove("-march=native")  # macOS issues with this flag
    extra_compile_args.append("-march=x86-64")

extensions = [
    Extension(
        "deepwater.io.reader_fast",
        ["src/deepwater/io/reader_fast.pyx"],
        extra_compile_args=extra_compile_args,
        extra_link_args=["-O3"],
    )
]

setup(
    ext_modules=cythonize(
        extensions,
        compiler_directives={
            'language_level': "3",
            'boundscheck': False,      # No bounds checking
            'wraparound': False,        # No negative indexing
            'cdivision': True,          # C-style division
            'initializedcheck': False,  # No initialization checks
            'embedsignature': True,     # Keep docstrings
        },
        annotate=False,  # Set to True to generate HTML annotation files
    ),
)
