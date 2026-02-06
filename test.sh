#!/usr/bin/env bash
# Test runner for Deepwater
set -e

echo "🧪 Running Deepwater Test Suite"
echo ""

# Activate venv if it exists
if [ -d ".venv/bin" ]; then
    source .venv/bin/activate
fi

# Run the test suite
python tests/__init__.py "$@"
