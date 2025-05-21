#!/bin/bash
# Setup script for development environment

set -e

echo "Setting up development environment..."

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Copy environment file if it doesn't exist
if [ ! -f ".env" ]; then
    echo "Creating .env file from example..."
    cp .env.example .env
fi

# Generate sample logs
echo "Generating sample logs..."
python scripts/generate_sample_logs.py --jobs 10 --days 5 --output sample_logs.json

echo "Development environment setup complete!"
echo
echo "To activate the environment, run: source venv/bin/activate"
echo "To start the application, run: uvicorn app.main:app --reload"
echo "To ingest sample logs, run: python scripts/ingest_sample_logs.py --file sample_logs.json"
echo 