# Options Whale Tracker

Streamlit dashboard that scans S&P 500 option chains and highlights unusual
activity. It supports two data sources:

- yfinance — quick, no API key required, used for the "Top S&P 500 Options"
  tab
- Charles Schwab Market Data API (via `schwab-py`) — optional, used for the
  "Schwab Whale Screener" tab (requires a Schwab developer app)

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

Makefile targets

This repo includes a `Makefile` that wraps common Docker and test tasks. Run
from the project root:

```bash
make help        # list available targets
make build       # build the Docker image (docker compose build)
make run         # run the stack (docker compose up); requires a filled .env
make stop        # stop and remove containers (docker compose down)
make logs        # tail container logs
make rebuild     # build with --no-cache
make clean       # stop and remove the image
make news-test   # smoke-test Yahoo Finance news client (uses .venv or docker)
```

Examples

```bash
# build and run in background
make build && docker compose up -d

# run the app in foreground (Ctrl-C to stop)
make run

# run the Yahoo Finance news smoke-test for AAPL and NVDA
SYMS="AAPL NVDA" make news-test
```


