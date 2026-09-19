#!/usr/bin/env bash
# ============================================================
# Build the Lambda deployment package for download_api.py.
#
# Produces aws-lambda/package/ containing:
#   - download_api.py            (the actual Lambda code, unmodified)
#   - requests/ and its deps     (the one third-party dependency it needs)
#
# Usage: ./build_deployment_package.sh
# Then:  sam build && sam deploy --guided   (see README.md)
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PACKAGE_DIR="$SCRIPT_DIR/package"

echo "Cleaning previous package..."
rm -rf "$PACKAGE_DIR"
mkdir -p "$PACKAGE_DIR"

echo "Copying Lambda source (src/api/download_api.py)..."
cp "$PROJECT_ROOT/src/api/download_api.py" "$PACKAGE_DIR/"

echo "Installing third-party dependencies into the package..."
pip install -r "$SCRIPT_DIR/requirements.txt" \
    --target "$PACKAGE_DIR" \
    --platform manylinux2014_x86_64 \
    --python-version 3.12 \
    --only-binary=:all: \
    --upgrade

echo "Package built at: $PACKAGE_DIR"
echo "Next: sam build && sam deploy --guided"
