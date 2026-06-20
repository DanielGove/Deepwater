"""
Setup script for Deepwater with Cython extensions.
Compiles hot paths to C for maximum performance.
"""
from __future__ import annotations

import sys

from Cython.Build import cythonize
from setuptools import Extension, setup


extra_compile_args = [
    "-O3",
    "-march=native",
    "-ffast-math",
]

if sys.platform == "darwin":
    extra_compile_args.remove("-march=native")
    extra_compile_args.append("-march=x86-64")


extensions = [
    Extension(
        "deepwater.io.reader.reader",
        ["deepwater/io/reader/reader.py"],
        extra_compile_args=extra_compile_args,
        extra_link_args=["-O3"],
    ),
    Extension(
        "deepwater.io.reader.traversal",
        ["deepwater/io/reader/traversal.pyx"],
        extra_compile_args=extra_compile_args,
        extra_link_args=["-O3"],
    ),
    Extension(
        "deepwater.io.ring_shadow",
        ["deepwater/io/ring_shadow.pyx"],
        extra_compile_args=extra_compile_args,
        extra_link_args=["-O3"],
    ),
    Extension(
        "deepwater.utils.timestamps",
        ["deepwater/utils/timestamps.pyx"],
        extra_compile_args=extra_compile_args,
        extra_link_args=["-O3"],
    ),
]


setup(
    ext_modules=cythonize(
        extensions,
        build_dir="build/cython",
        compiler_directives={
            "language_level": "3",
            "boundscheck": False,
            "wraparound": False,
            "cdivision": True,
            "initializedcheck": False,
            "embedsignature": True,
        },
        annotate=False,
    ),
    zip_safe=False,
)
