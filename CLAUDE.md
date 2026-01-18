# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

AI Stock Watcher is a full-stack intelligent stock monitoring system combining quantitative data analysis with AI-powered trading recommendations for Chinese stock markets. It uses AkShare as the data source and integrates with multiple LLM providers (OpenAI, SiliconFlow, DeepSeek) to generate trading signals.

**Tech Stack:**
- **Backend**: FastAPI + SQLAlchemy + SQLite, with APScheduler for monitoring jobs
- **Frontend**: React 19 + TypeScript + Vite + Ant Design
- **Data**: AkShare (Chinese stock market API)
- **AI**: OpenAI-compatible API (supports multiple providers)

## Development Commands

### Backend
```bash
cd backend
pip install -r requirements.txt
python init_db.py        # Initialize/recreate database (CAUTION: rebuilds stock_watch.db)
python3 -m uvicorn main:app --reload --port 8000
```

### Frontend
```bash
cd frontend
npm install
npm run dev              # Dev server at http://localhost:5173
npm run build            # Production build
npm run lint             # Run ESLint
```

### Database Notes
- Database file: `backend/stock_watch.db`
- The app requires the DB file to exist before startup (enforced in `database.py`)
- Schema migrations are handled automatically on startup via `ensure_db_schema()` in `main.py`
- When adding new columns, update the migration logic in `main.py`

## Architecture Overview

### Database Schema (SQLite)
- **Stocks**: Stock symbols with monitoring config (schedule, AI provider, pinned status)
- **IndicatorDefinitions**: Reusable technical indicators using AkShare APIs or Python scripts
- **Stock_Indicators**: Many-to-many relationship for stock-indicator mapping
- **AIConfigs**: Multiple LLM provider configurations (supports OpenAI, SiliconFlow, DeepSeek)
- **RuleScripts**: Custom Python scripts for rule-based monitoring
- **Logs**: Historical monitoring data and AI analysis results
- **StockAIWatchConfigs**: Per-stock AI watch settings (mode, prompts, script selection)

### Service Layer (`backend/services/`)
- **monitor_service.py**: Core scheduling and monitoring logic (APScheduler jobs, stock analysis orchestration)
- **ai_service.py**: LLM integration with custom system prompts for quantitative trading, JSON output parsing
- **data_fetcher.py**: Handles AkShare API calls and custom Python indicator script execution
- **alert_service.py**: Email notifications via SMTP
- **screener_service.py**: Market screening and stock selection workflows
- **streamlit_service.py**: Manages the Streamlit dashboard subprocess

### API Routers (`backend/routers/`)
- **stocks**: CRUD operations, monitoring control, manual analysis
- **ai_configs**: LLM provider management
- **indicators**: Technical indicator definitions (AkShare API or Python script mode)
- **logs**: Historical data viewing with filtering
- **screeners**: Market screening workflows
- **rules**: Custom rule script management
- **research**: Research and analysis tools
- **news**: News and sentiment analysis

### Frontend Structure (`frontend/src/`)
- **App.tsx**: Main routing setup
- **pages/**: Main pages (Stocks, AIConfig, Indicators, Logs, Settings, etc.)
- **components/**: Reusable UI components
- **api/**: Axios client and TypeScript types for backend endpoints
- **assets/**: Theme configuration

## Key Concepts

### Indicator System
Indicators can be configured in two modes:
1. **AkShare API Mode**: Specify `akshare_api` name and `params_json` (supports `{symbol}` and `{today}` placeholders)
2. **Python Script Mode**: Provide custom `python_code` that has access to `pandas`, `numpy`, `akshare` libraries

Indicators also support `post_process_json` for data transformation after fetching.

### AI Analysis Modes
- **AI Mode**: Pure AI analysis using LLM
- **Script Mode**: Custom rule-based monitoring using Python scripts
- **Hybrid Mode**: Combined AI + script analysis

### Monitoring Schedule
- Stocks can be monitored on schedules (e.g., `*/5 * * * *` for every 5 minutes)
- `only_trade_days` flag limits monitoring to trade days only
- Jobs are managed via APScheduler in `monitor_service.py`

### AI Service Architecture
The AI service enforces strict JSON output from LLMs with a predefined schema:
- Fields: `type`, `signal`, `action_advice`, `suggested_position`, `duration`, `support_pressure`, `stop_loss_price`, `message`
- Signal types: `STRONG_BUY`, `BUY`, `WAIT`, `SELL`, `STRONG_SELL`
- Type types: `info`, `warning`, `error`

For non-OpenAI providers (SiliconFlow, DeepSeek), the system prompt is embedded in the user message for compatibility.

## Environment Configuration

Create `backend/.env` for optional email alerts:
```
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SENDER_EMAIL=your_email@gmail.com
SENDER_PASSWORD=your_password
RECEIVER_EMAIL=your_email@gmail.com
```

## Startup Sequence

On backend startup (`main.py`):
1. `ensure_db_schema()` - Creates tables and adds missing columns
2. `start_scheduler()` - Initializes APScheduler for monitoring jobs
3. `restore_screener_jobs()` - Restores persistent screener jobs
4. `start_streamlit()` - Launches the Streamlit dashboard subprocess

## Important Notes

- **Language**: The application and UI are primarily in Chinese (stock symbols, prompts, AI responses)
- **Data Source**: AkShare provides Chinese stock market data
- **AI Prompts**: The system prompt is in Chinese, designed for quantitative fund manager persona
- **Database Safety**: The app will refuse to start if `stock_watch.db` doesn't exist (prevents accidental data loss)
- **Indicator Context**: When fetching indicators, the context includes `symbol` and `name` for parameter substitution
