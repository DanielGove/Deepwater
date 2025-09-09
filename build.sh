#!/usr/bin/env bash
set -euo pipefail
ver="$1" # e.g. 0.1.3
sed -i "s/^version = .*/version = \"$ver\"/" pyproject.toml
git commit -am "release: v$ver"
git tag "v$ver"
python -m pip install --upgrade build setuptools wheel
python -m build
cp dist/deepwater-$ver-*.whl /deepwater/repos/wheelhouse/
rm -rf dist/
git push && git push --tags
echo "Built deepwater-$ver and copied to wheelhouse."