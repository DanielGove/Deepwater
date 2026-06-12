#!/usr/bin/env bash
set -e

if [ -x ".venv/bin/python" ]; then
    PYTHON_BIN=".venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="python"
else
    echo "test.sh: python3 or python is required" >&2
    exit 127
fi

if [ -z "${TMPDIR:-}" ] && [ -d "/dev/shm" ] && [ -w "/dev/shm" ]; then
    export TMPDIR="/dev/shm"
fi

export PYTHONDONTWRITEBYTECODE="${PYTHONDONTWRITEBYTECODE:-1}"

"$PYTHON_BIN" -m pytest -p no:cacheprovider "$@"
