#!/bin/bash

# Script to set up Python virtual environment for Azure Function

echo "Setting up virtual environment for Notification Engine..."

# Create virtual environment
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install requirements
pip install -r requirements.txt

echo "Virtual environment setup complete!"
echo "To activate: source .venv/bin/activate"

