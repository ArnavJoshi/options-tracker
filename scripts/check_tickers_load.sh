#!/usr/bin/env bash
set -euo pipefail

echo "Running generator runner..."
bash scripts/generate_tickers_runner.sh

echo
echo "Checking for .tickers_all.txt at repo root:"
if [ -f .tickers_all.txt ]; then
  echo "FOUND: $(pwd)/.tickers_all.txt"
  echo "Head (first 10 lines):"
  head -n 10 .tickers_all.txt || true
else
  echo "NOT FOUND at repo root"
fi

echo
echo "Running Python path-check to mirror app logic..."
python3 - <<'PY'
from pathlib import Path
repo_candidate = Path('app.py').resolve().parent / '.tickers_all.txt'
cwd_candidate = Path.cwd() / '.tickers_all.txt'
print('repo_candidate:', repo_candidate)
print('  exists:', repo_candidate.exists())
print('cwd_candidate:', cwd_candidate)
print('  exists:', cwd_candidate.exists())
if repo_candidate.exists():
    print('App would load from repo_candidate')
elif cwd_candidate.exists():
    print('App would load from cwd_candidate')
else:
    print('No .tickers_all.txt found by app logic; fallback to S&P 500')
PY

