#!/usr/bin/env bash
set -e

if [ -d ".venv/bin" ]; then
    source .venv/bin/activate
fi

if [ -z "${TMPDIR:-}" ] && [ -d "/dev/shm" ] && [ -w "/dev/shm" ]; then
    export TMPDIR="/dev/shm"
fi

export PYTHONDONTWRITEBYTECODE="${PYTHONDONTWRITEBYTECODE:-1}"

python -m pytest -p no:cacheprovider "$@"
