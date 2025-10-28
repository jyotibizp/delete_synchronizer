#!/bin/bash

# Setup virtual environment for delete_sync function

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "Setting up virtual environment for delete_sync..."

# Create venv
python3 -m venv .venv

# Activate and install
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo "✓ Virtual environment setup complete"
echo ""
echo "To activate:"
echo "  source .venv/bin/activate"

