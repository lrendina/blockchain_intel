#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

docker exec -it base-intel-db psql -U trader -d base_mainnet
