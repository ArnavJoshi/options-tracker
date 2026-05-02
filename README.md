# Options Whale Tracker

Streamlit dashboard that scans NASDAQ, NYSE, and AMEX option chains and highlights unusual
activity using yfinance and Yahoo Finance data.

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

`make run` works without API credentials. Optional local overrides can still be
placed in `.env` if you want Docker Compose to load them automatically.

## Features
- Scan NASDAQ/NYSE/AMEX ticker universes from all_tickers.txt
- yfinance-based top-options view (no API key)
- Multi-column ranking (volume, open interest, vol/OI ratio, IV, etc.)
- ITM/ATM/OTM classification, per-symbol filtering, and color-coded display
- Yahoo Finance headlines (cached) and per-contract drilldown

## Setup

### Install & run locally
Create and activate a virtualenv, install deps:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

No API keys are required for the current app workflows.

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
- Large all-ticker scans can still be slow; batching and cached prefilters help.
- News is cached for 5 minutes; option/quote data uses short-lived process caches.

## Project layout

```
app.py
data/ (tickers, news_client, yfinance_options)
requirements.txt
.env.example
Dockerfile
docker-compose.yml
```

The repository includes a `Dockerfile` and `docker-compose.yml` for local
containerized runs. The compose stack mounts `./.cache` so generated cache
files persist across restarts.

Useful commands:

```bash
docker compose up -d            # run detached
docker compose logs -f          # follow logs
docker compose restart          # restart after editing .env
docker compose down             # stop + remove container
docker compose build --no-cache # rebuild from scratch
```

<!-- Makefile targets moved to Quick start above -->



## Ticker universe (all_tickers.txt)

The app sources its full NASDAQ/NYSE/AMEX universe from `all_tickers.txt` in
the project root — one symbol per line. `data/tickers.py` reads it at startup
with no generation step required.

The file is sourced from:
**[rreichel3/US-Stock-Symbols](https://github.com/rreichel3/US-Stock-Symbols/blob/main/all/all_tickers.txt)**

To update, download the latest version of that file and replace `all_tickers.txt`
in the repo root, or edit it directly to add/remove tickers.
