
# PowerShell script to set up Python virtual environment and install dependencies

# Create virtual environment
python -m venv venv

# Activate virtual environment
.env\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
