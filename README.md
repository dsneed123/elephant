# Elephant 🐘

Kalshi prediction market copy-trading platform. Track top bettors, analyze performance, and mirror winning trades.

## What It Does

1. **Track** — Scrapes Kalshi leaderboard and monitors top 0.01% bettors
2. **Analyze** — Scores traders by win rate, consistency, ROI, and market diversity
3. **Copy** — Mirrors top traders' positions via Kalshi API with configurable risk management
4. **Dashboard** — Real-time web UI showing tracked traders, signals, and portfolio performance

## Architecture

- **Backend**: Python + FastAPI
- **Frontend**: React + TypeScript + Vite
- **Data**: SQLite (local) / PostgreSQL (prod)
- **Kalshi Integration**: REST API + WebSocket for real-time order book
- **SDK**: `kalshi_python_sync` official SDK

## Quick Start

```bash
# Backend
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload

# Frontend
cd frontend
npm install
npm run dev
```

## Environment Variables

```
KALSHI_API_KEY=your_key_id
KALSHI_PRIVATE_KEY_PATH=path/to/private_key.pem
DATABASE_URL=sqlite:///./elephant.db
```
