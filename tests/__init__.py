"""
Deepwater Test Suite
--------------------
Minimal testing framework without external dependencies.
Run with: ./test.sh
"""
import sys
import os
from pathlib import Path

# Add repo root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def _deepwater_shm_files() -> set[Path]:
    shm_dir = Path("/dev/shm")
    if not shm_dir.exists():
        return set()
    return set(shm_dir.glob("dw_*"))


def _cleanup_new_shm(before: set[Path]) -> None:
    for path in _deepwater_shm_files() - before:
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def run_test_file(test_file: Path) -> tuple[bool, str]:
    """Run a single test file and return (success, output)"""
    import subprocess

    shm_before = set()
    try:
        shm_before = _deepwater_shm_files()
        env = os.environ.copy()
        root_path = str(Path(__file__).parent.parent)
        env["PYTHONPATH"] = root_path if not env.get("PYTHONPATH") else f"{root_path}:{env['PYTHONPATH']}"
        result = subprocess.run(
            [sys.executable, str(test_file)],
            capture_output=True,
            text=True,
            timeout=30,
            cwd=test_file.parent,
            env=env,
        )
        
        success = result.returncode == 0
        output = result.stdout + result.stderr
        return success, output
    except subprocess.TimeoutExpired:
        return False, "TIMEOUT (>30s)"
    except Exception as e:
        return False, f"ERROR: {e}"
    finally:
        _cleanup_new_shm(shm_before)


def run_all_tests():
    """Discover and run all test files"""
    test_root = Path(__file__).parent
    project_root = test_root.parent
    
    # Find all test files
    test_files = []
    
    # Core tests (./tests/)
    test_files.extend(sorted(test_root.glob("test_*.py")))
    
    # Root level tests
    test_files.extend(sorted(project_root.glob("test_*.py")))
    
    if not test_files:
        print("❌ No test files found")
        return False
    
    print("=" * 70)
    print(f"Deepwater Test Suite - Found {len(test_files)} test(s)")
    print("=" * 70)
    
    results = []
    
    for test_file in test_files:
        test_name = test_file.stem
        relative_path = test_file.relative_to(project_root)
        
        print(f"\n▶ {test_name} ({relative_path})")
        print("-" * 70)
        
        success, output = run_test_file(test_file)
        results.append((test_name, success, output))
        
        if success:
            print("✅ PASS")
        else:
            print("❌ FAIL")
            # Show last 20 lines of output on failure
            lines = output.strip().split('\n')
            for line in lines[-20:]:
                print(f"  {line}")
    
    # Summary
    print("\n" + "=" * 70)
    print("Test Summary")
    print("=" * 70)
    
    passed = sum(1 for _, success, _ in results if success)
    failed = len(results) - passed
    
    for test_name, success, _ in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} - {test_name}")
    
    print(f"\nTotal: {passed}/{len(results)} passed")
    
    if failed > 0:
        print(f"\n❌ {failed} test(s) failed")
        return False
    else:
        print(f"\n✅ All tests passed!")
        return True


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
