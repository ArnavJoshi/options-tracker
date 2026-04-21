# Options Whale Tracker

A local Streamlit dashboard that screens S&P 500 options via the **Charles Schwab
Market Data API** and flags potentially "whale-driven" contracts using a 20-day
volume-spike heuristic, then overlays related company news from **Yahoo Finance**
(via `yfinance`).

> ⚠️ TradingView has **no public options API**. This project uses Schwab's
> official developer API (OAuth 2.0). You need a Schwab brokerage account and a
> registered developer app. News comes from Yahoo Finance and requires no key.

## Features
- 📊 Scans the most active S&P 500 underliers (configurable, default 50, max 500).
- 🐳 "Whale" flag when current contract volume ≥ `multiplier × 20-day avg volume`
  (default 5×) — a relaxation of the original "≥3000 single trade" rule because
  Schwab's public data API does not expose option time & sales.
- 🔎 Filters: side (call/put/both), min volume, min open interest, expiration window.
- 📰 Top 3 recent news headlines per symbol (Yahoo Finance, no API key needed).
- ⏱ Auto-refresh every 30 seconds (toggle in sidebar).
- 🧊 Per-contract drilldown with volume-vs-20d-avg chart and news list.

## Setup

### 1. Register a Schwab developer app
1. Sign in at https://developer.schwab.com and create an app.
2. Enable the **Accounts and Trading** + **Market Data Production** products.
3. Set the callback URL to `https://127.0.0.1:8182` (must match `SCHWAB_CALLBACK_URL`).
4. Wait for the app status to become **Ready For Use**.
5. Copy the **App Key** and **App Secret**.

### 2. Install + configure
```bash
cd /Users/arjoshi/personal/untitled
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# then edit .env:
#   SCHWAB_APP_KEY=...
#   SCHWAB_APP_SECRET=...
#   SCHWAB_CALLBACK_URL=https://127.0.0.1:8182
# (news is via Yahoo Finance — no key required)
```

### 3. Run
```bash
streamlit run app.py
```

**First launch** opens a browser window for Schwab OAuth. After approving,
you'll be redirected to a `https://127.0.0.1:8182/...` URL that won't load —
copy the full redirected URL back into the terminal prompt when `schwab-py`
asks. The refresh token is saved to `.cache/schwab_token.json`; subsequent
launches are silent until the refresh token expires (~7 days). When it expires,
delete the token file and repeat.

The dashboard opens at http://localhost:8501.

## Notes & Caveats
- Schwab refresh tokens expire every **7 days** — expect to re-auth weekly.
- The 20-day avg history call is the slowest part. Uncheck *"Compute 20d avg volume"*
  in the sidebar for faster scans (you lose whale flagging).
- News fetch is cached for 5 minutes; chain/quote data is cached ~20–25 seconds
  to align with the 30s refresh.- Symbol universe (S&P 500) is cached for 24 hours in `.cache/sp500.json`.
- Schwab rate limits are ~120 req/min per endpoint; the client applies a soft
  20 rps cap and retries on 429.

## Project Layout
```
app.py                       # Streamlit dashboard
data/
  sp500.py                   # S&P 500 ticker fetch + cache
  universe.py                # rank top-N most active S&P 500 names
  schwab_client.py           # Schwab market data wrapper (quotes, chains, history)
  news_client.py             # Yahoo Finance news (yfinance)
screener/
  engine.py                  # concurrent scan + whale-flag logic
requirements.txt
.env.example
Dockerfile
docker-compose.yml
```

## Run in Docker

A `Dockerfile` and `docker-compose.yml` are provided for fully local containerized runs.

### 1. Prepare `.env` and cache dir
```bash
cp .env.example .env   # fill SCHWAB_APP_KEY / SCHWAB_APP_SECRET (news needs no key)
mkdir -p .cache
```

The compose file mounts `./.cache` into the container so the Schwab refresh
token (`.cache/schwab_token.json`) survives restarts.

### 2. Build and run
```bash
docker compose up --build
```

Open http://localhost:8501.

### 3. First-run Schwab OAuth (inside the container)
On first launch the container has no token file, so `schwab-py` starts a local
web server on port **8182** and waits for the Schwab callback. The compose file
publishes that port to the host.

1. Watch the container logs: `docker compose logs -f options-tracker`.
2. `schwab-py` prints a Schwab authorization URL. Open it in your browser and log in.
3. Schwab redirects to `https://127.0.0.1:8182/...`. Your browser talks to the
   published port on the host, which forwards to the container. The container
   captures the code, exchanges it for a token, and writes
   `.cache/schwab_token.json`.
4. The Streamlit page at http://localhost:8501 then starts loading normally.

> If your browser blocks the self-signed `https://127.0.0.1:8182` page, click
> "Advanced → Proceed anyway" — it's only used for the redirect.

### 4. Subsequent runs
Refresh token is reused from the mounted volume. Schwab refresh tokens expire
after ~7 days — when you start seeing auth errors, stop the stack, delete
`.cache/schwab_token.json`, and run `docker compose up` again to redo step 3.

### Useful commands
```bash
docker compose up -d            # run detached
docker compose logs -f          # follow logs
docker compose restart          # restart after editing .env
docker compose down             # stop + remove container
docker compose build --no-cache # rebuild from scratch
```


