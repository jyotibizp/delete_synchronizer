#!/bin/bash
# Bash script to set up Python virtual environment and install dependencies (macOS/Linux)

# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

echo "✅ Virtual environment created and dependencies installed"
echo "To activate the environment, run: source venv/bin/activate"

