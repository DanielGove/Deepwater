#!/usr/bin/env python3
"""
Test script simulating usage from an external repo.
Validates that docs/help are accessible and useful.
"""

from deepwater import Platform
import time

def test_module_doc():
    """Test module-level documentation"""
    import deepwater
    assert 'Zero-copy' in deepwater.__doc__
    assert '60µs' in deepwater.__doc__
    assert 'Quick Start' in deepwater.__doc__
    print("✓ Module docs OK")

def test_class_docs():
    """Test class-level documentation"""
    assert 'Platform' in Platform.__doc__
    assert '60µs' in Platform.__doc__
    assert 'Performance' in Platform.__doc__
    
    from deepwater import Reader, Writer
    assert 'Zero-copy reader' in Reader.__doc__
    assert '70µs' in Reader.__doc__
    assert 'Writer for persistent' in Writer.__doc__
    print("✓ Class docs OK")

def test_method_docs():
    """Test method-level documentation"""
    from deepwater import Reader
    
    # Check stream() docs
    assert 'Stream records' in Reader.stream.__doc__
    assert '70µs' in Reader.stream.__doc__
    assert 'start=None' in Reader.stream.__doc__
    
    # Check range() docs
    assert 'historical' in Reader.range.__doc__.lower()
    assert '920K' in Reader.range.__doc__
    
    # Check latest() docs
    assert 'recent' in Reader.latest.__doc__.lower()
    assert 'convenience' in Reader.latest.__doc__.lower()
    
    print("✓ Method docs OK")

def test_help_output():
    """Test that help() produces useful output"""
    import io
    import sys
    from contextlib import redirect_stdout
    
    # Capture help(Platform) output
    f = io.StringIO()
    with redirect_stdout(f):
        help(Platform)
    output = f.getvalue()
    
    # Verify key sections present
    assert 'Performance:' in output
    assert '60µs' in output
    assert 'Example:' in output
    assert 'create_feed' in output
    
    print("✓ help(Platform) OK")

def test_quick_example():
    """Test the quick example from docs actually works"""
    import tempfile
    import shutil
    
    tmpdir = tempfile.mkdtemp()
    try:
        # Quick example from docs
        p = Platform(tmpdir)
        
        p.create_feed({
            'feed_name': 'trades',
            'mode': 'UF',
            'fields': [
                {'name': 'price', 'type': 'float64'},
                {'name': 'size', 'type': 'float64'},
                {'name': 'timestamp_us', 'type': 'uint64'},
            ],
            'ts_col': 'timestamp_us',
            'persist': True,
        })
        
        # Write
        writer = p.create_writer('trades')
        ts = int(time.time() * 1e6)
        writer.write_values(123.45, 100.0, ts)
        writer.close()
        
        # Read
        reader = p.create_reader('trades')
        records = reader.latest(60)
        assert len(records) == 1
        assert records[0][0] == 123.45
        reader.close()
        
        p.close()
        print("✓ Quick example works")
        
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

if __name__ == '__main__':
    print("Testing Deepwater documentation...")
    print()
    
    test_module_doc()
    test_class_docs()
    test_method_docs()
    test_help_output()
    test_quick_example()
    
    print()
    print("=" * 50)
    print("✓ All documentation tests passed!")
    print()
    print("Users can now:")
    print("  - import deepwater")
    print("  - help(Platform) for full docs")
    print("  - help(Reader.stream) for method docs")
    print("  - See README.md for comprehensive guide")
    print("  - See QUICK_REFERENCE.md for cheat sheet")
