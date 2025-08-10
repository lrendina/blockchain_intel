#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Ensure .env exists
if [[ ! -f .env ]]; then
  echo "Missing .env in $(pwd). Please create it and set QUICKNODE_BASE_URL." >&2
  exit 1
fi

/bin/bash -lc 'set -a && source .env && set +a && python3 pipeline/base_pipeline_best.py'
