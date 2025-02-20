#!/bin/bash
# chmod +x setup.sh
# ./setup.sh
# Stop script on error
set -e

echo "🚀 Setting up the project..."

# Check if Python is installed
if ! command -v python3 &> /dev/null
then
    echo "❌ Python3 is not installed. Please install Python 3 and retry."
    exit 1
fi

# Set up virtual environment
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
else
    echo "✅ Virtual environment already exists."
fi

# Activate virtual environment
echo "🐍 Activating virtual environment..."
source venv/bin/activate

# Install dependencies
if [ -f "requirements.txt" ]; then
    echo "📥 Installing dependencies from requirements.txt..."
    pip install --upgrade pip
    pip install -r requirements.txt
else
    echo "⚠️ No requirements.txt found. Skipping dependency installation."
fi

# Ensure 'data/' directory exists
if [ ! -d "data" ]; then
    echo "📁 Creating empty 'data/' directory..."
    mkdir -p "data"
else
    echo "✅ 'data/' directory already exists."
fi

echo "🎉 Setup complete!"
echo "➡️ To start working, activate the virtual environment using:"
echo "   source venv/bin/activate"
