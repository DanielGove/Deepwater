#!/usr/bin/env python3
"""Layout validation tests (clock_level model)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from deepwater.metadata.layout_json import build_layout


def _raises(msg_sub, fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except Exception as e:
        assert msg_sub in str(e), f"expected '{msg_sub}' in '{e}'"
        return
    raise AssertionError("expected exception but none raised")


def test_clock_level_required():
    fields = [{"name": "ts", "type": "uint64"}]
    _raises("clock_level is required", build_layout, fields)


def test_clock_level_range():
    fields = [{"name": "ts", "type": "uint64"}]
    _raises("between 1 and 3", build_layout, fields, clock_level=0)
    _raises("between 1 and 3", build_layout, fields, clock_level=4)


def test_leading_uint64_enforced():
    fields = [{"name": "ts", "type": "float64"}]
    _raises("requires field 'ts' to be uint64", build_layout, fields, clock_level=1)


def test_insufficient_fields():
    fields = [{"name": "ts", "type": "uint64"}]
    _raises("requires at least 2 fields", build_layout, fields, clock_level=2)


def test_struct_size_matches_dtype():
    fields = [
        {"name": "ts", "type": "uint64"},
        {"name": "val", "type": "float64"},
    ]
    lay = build_layout(fields, clock_level=1, aligned=False)
    import struct
    assert struct.Struct(lay["fmt"]).size == lay["record_size"], "struct size mismatch"


def run_tests():
    tests = [
        test_clock_level_required,
        test_clock_level_range,
        test_leading_uint64_enforced,
        test_insufficient_fields,
        test_struct_size_matches_dtype,
    ]
    print("Layout Validation Tests")
    print("=" * 60)
    passed = 0
    for t in tests:
        try:
            t()
            print(f"✅ {t.__name__}")
            passed += 1
        except Exception as e:
            print(f"❌ {t.__name__} - {e}")
    print(f"\nPassed: {passed}/{len(tests)}")
    if passed != len(tests):
        sys.exit(1)


if __name__ == "__main__":
    run_tests()
