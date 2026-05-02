#!/usr/bin/env bash
set -euo pipefail

echo "Checking for all_tickers.txt at repo root:"
if [ -f all_tickers.txt ]; then
  echo "FOUND: $(pwd)/all_tickers.txt"
  echo "Head (first 10 lines):"
  head -n 10 all_tickers.txt || true
else
  echo "NOT FOUND at repo root"
fi

echo
echo "Running Python path-check to mirror app logic..."
python3 - <<'PY'
from pathlib import Path
repo_candidate = Path('app.py').resolve().parent / 'all_tickers.txt'
cwd_candidate = Path.cwd() / 'all_tickers.txt'
print('repo_candidate:', repo_candidate)
print('  exists:', repo_candidate.exists())
print('cwd_candidate:', cwd_candidate)
print('  exists:', cwd_candidate.exists())
if repo_candidate.exists():
    print('App would load from repo_candidate')
elif cwd_candidate.exists():
    print('App would load from cwd_candidate')
else:
    print('No all_tickers.txt found by app logic; using fallback list')
PY

