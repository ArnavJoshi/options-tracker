# Options Whale Tracker

Streamlit dashboard that scans S&P 500 option chains and highlights unusual
activity. It supports two data sources:

- yfinance — quick, no API key required, used for the "Top S&P 500 Options"
  tab
- Charles Schwab Market Data API (via `schwab-py`) — optional, used for the
  "Schwab Whale Screener" tab (requires a Schwab developer app)

Quick start (Docker / Makefile)

Prerequisites: Docker and Docker Compose (or Docker Desktop) installed on your machine.

From the project root you can use the provided `Makefile` to build and run the
app. This is the easiest path for non-developers:

```bash
# build the Docker image (required once or after changes)
make build

# run the app in the foreground (Ctrl-C to stop)
make run

# run in background (build first or use make build &&)
docker compose up -d
```

`make run` expects a populated `.env` (copy `.env.example` → `.env`) in the
project root. See the example `.env` below.

## Features
- Scan S&P 500 underliers (configurable size)
- yfinance-based top-options view (no API key)
- Schwab-backed whale screener (requires Schwab developer credentials)
- Multi-column ranking (volume, open interest, vol/OI ratio, IV, etc.)
- ITM/ATM/OTM classification, per-symbol filtering, and color-coded display
- Yahoo Finance headlines (cached) and per-contract drilldown

## Setup

### Schwab (optional)
If you want the Schwab-backed screener you must register a developer app at
https://developer.schwab.com and obtain an App Key + App Secret. Set
`SCHWAB_APP_KEY`, `SCHWAB_APP_SECRET`, and `SCHWAB_CALLBACK_URL` in `.env`.

### Install & run locally
Create and activate a virtualenv, install deps, copy the `.env` template:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# edit .env to add Schwab credentials if you plan to use the Schwab tab
```

Example `.env` (minimal)

```bash
# Optional: Schwab credentials for the Schwab Whale Screener
SCHWAB_APP_KEY=your_schwab_app_key_here
SCHWAB_APP_SECRET=your_schwab_app_secret_here
SCHWAB_CALLBACK_URL=https://127.0.0.1:8182
SCHWAB_TOKEN_PATH=.cache/schwab_token.json

# Streamlit settings (optional)
STREAMLIT_SERVER_PORT=8501
```

### Run

Run locally with Streamlit:

```bash
streamlit run app.py
```

Or run in Docker (recommended for repeatable environment):

```bash
docker compose up --build
```

Streamlit serves the dashboard at http://localhost:8501

## Notes & Caveats
### Notes & caveats
- Schwab refresh tokens expire (~7 days) — you may need to re-auth periodically.
- Computing historical 20-day averages is slower; you can disable that option
  in the UI for faster scans (you lose the whale-flagging heuristic).
- News is cached for 5 minutes; option/quote data uses short-lived process caches.

## Project layout

```
app.py
data/ (sp500, universe, schwab_client, news_client, yfinance options)
screener/ (engine)
requirements.txt
.env.example
Dockerfile
docker-compose.yml
```

The repository includes a `Dockerfile` and `docker-compose.yml` for local
containerized runs. The compose stack mounts `./.cache` so any Schwab token
persists across restarts.

Useful commands:

```bash
docker compose up -d            # run detached
docker compose logs -f          # follow logs
docker compose restart          # restart after editing .env
docker compose down             # stop + remove container
docker compose build --no-cache # rebuild from scratch
```

<!-- Makefile targets moved to Quick start above -->


