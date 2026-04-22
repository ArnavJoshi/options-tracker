#!/usr/bin/env bash
set -euo pipefail

echo "Generating .tickers_all.txt from public sources..."

# Prefer a project virtualenv if present
if [ -x .venv/bin/python ]; then
  if ! .venv/bin/python scripts/generate_tickers_all.py; then
    ret=$?
    echo "ERROR: generator failed in venv (exit=$ret)"
    exit $ret
  fi
  exit 0
fi

# Otherwise run inside a container that mounts the repo so /app/scripts exists
# Ensure the image is built so the runtime and deps are available.
# Use "docker compose" to match the Makefile's behaviour.
set +e
docker compose build options-tracker >/dev/null 2>&1
set -e

if ! docker run --rm -v "$(pwd)":/app -w /app options-tracker python3 scripts/generate_tickers_all.py; then
  echo "ERROR: failed to generate .tickers_all.txt inside container"
  exit 1
fi

exit 0

