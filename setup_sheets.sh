#!/bin/bash
# Quick setup script for Google Sheets export functionality

echo "======================================================================="
echo "Google Sheets Export - Quick Setup"
echo "======================================================================="
echo ""

# Check if Python is installed
if ! command -v python &> /dev/null; then
    echo "❌ Error: Python is not installed"
    echo "   Please install Python 3.8 or higher"
    exit 1
fi

echo "✅ Python found: $(python --version)"
echo ""

# Install dependencies
echo "📦 Installing dependencies..."
pip install -r requirements.txt

if [ $? -ne 0 ]; then
    echo "❌ Failed to install dependencies"
    exit 1
fi

echo ""
echo "✅ Dependencies installed"
echo ""

# Run verification
echo "🔍 Running verification..."
python verify_setup.py

echo ""
echo "======================================================================="
echo "Next Steps:"
echo "======================================================================="
echo ""
echo "1. Set up Google Cloud credentials:"
echo "   Follow the guide: docs/GOOGLE_SHEETS_SETUP.md"
echo ""
echo "2. Once credentials are set up, run:"
echo "   python export_to_sheets.py"
echo ""
echo "======================================================================="
