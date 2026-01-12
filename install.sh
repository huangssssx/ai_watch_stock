#!/bin/bash

# AI Stock Watcher - Installation Script
# This script installs dependencies and initializes the database

set -e

echo "================================"
echo "AI Stock Watcher - Installation"
echo "================================"
echo ""

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed. Please install Python 3.7 or higher."
    exit 1
fi

# Check if Node.js is installed
if ! command -v node &> /dev/null; then
    echo "Error: Node.js is not installed. Please install Node.js 16 or higher."
    exit 1
fi

echo "✓ Python 3 found: $(python3 --version)"
echo "✓ Node.js found: $(node --version)"
echo ""

# Install backend dependencies
echo "Installing backend dependencies..."
cd backend
pip3 install -r requirements.txt
cd ..
echo "✓ Backend dependencies installed"
echo ""

# Install frontend dependencies
echo "Installing frontend dependencies..."
cd frontend
npm install
cd ..
echo "✓ Frontend dependencies installed"
echo ""

# Initialize database
echo "Initializing database..."
if [ -f "backend/init_db.py" ]; then
    cd backend
    python3 init_db.py
    cd ..
    echo "✓ Database initialized"
else
    echo "Warning: init_db.py not found. Please initialize the database manually."
fi
echo ""

# Create .env.example if it doesn't exist
if [ ! -f "backend/.env" ]; then
    echo "Creating .env.example file..."
    cat > backend/.env.example << 'EOF'
# Email Configuration (Optional)
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SENDER_EMAIL=your_email@gmail.com
SENDER_PASSWORD=your_password
RECEIVER_EMAIL=your_email@gmail.com

# OpenAI API Configuration (Optional)
OPENAI_API_KEY=your_openai_api_key
OPENAI_BASE_URL=https://api.openai.com/v1

# SiliconFlow API Configuration (Optional)
SILICONFLOW_API_KEY=your_siliconflow_api_key

# DeepSeek API Configuration (Optional)
DEEPSEEK_API_KEY=your_deepseek_api_key
EOF
    echo "✓ Created backend/.env.example"
    echo "  Please copy .env.example to .env and configure your API keys"
fi

echo ""
echo "================================"
echo "Installation Complete!"
echo "================================"
echo ""
echo "To start the application:"
echo ""
echo "1. Backend (Terminal 1):"
echo "   cd backend"
echo "   python3 -m uvicorn main:app --reload --port 8000"
echo ""
echo "2. Frontend (Terminal 2):"
echo "   cd frontend"
echo "   npm run dev"
echo ""
echo "Then open http://localhost:5173 in your browser."
echo ""
