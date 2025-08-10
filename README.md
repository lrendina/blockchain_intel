# Trader Intelligence – Docker Workflow

This project ingests, enriches, and stores Base blockchain data into Postgres. The canonical local setup uses Docker.

## Prerequisites
- Docker Desktop (macOS)
- Python 3.9+ (to run the pipeline)

## Quick start
```bash
# from blockchain_intel/
cp .env .env.local.example 2>/dev/null || true  # optional backup
# edit .env and set QUICKNODE_BASE_URL

make up         # start Postgres
make schema     # apply database/schema1.sql
make run        # run the pipeline once
```

Common commands
- `make up` – start Postgres container `base-intel-db`
- `make schema` – apply `database/schema1.sql` into the DB
- `make run` – run `pipeline/base_pipeline_best.py` with `.env`
- `make status` – show compose service status
- `make logs` – tail Postgres logs
- `make psql` – open a `psql` shell in the container
- `make down` – stop containers

## Environment
`.env` in `blockchain_intel/`:
```
QUICKNODE_BASE_URL=https://mainnet.base.org  # or your provider URL
DB_HOST=localhost
DB_PORT=5432
DB_NAME=base_mainnet
DB_USER=trader
DB_PASS=password
CONFIRMATIONS=5
```

## Notes
- The pipeline writes into `token_transfers` and uses `pipeline_state` for checkpointing.
- CoinGecko token list is cached locally (`coin_list.json`) to reduce API calls.
- Default confirmation delay is 5 blocks to avoid reorgs.

## Scripts
Scripts mirror the Makefile targets. From `blockchain_intel/`:
```bash
scripts/up.sh       # docker compose up -d
scripts/schema.sh   # apply schema
scripts/run.sh      # run pipeline with .env
scripts/status.sh   # docker compose ps
scripts/logs.sh     # docker logs -f base-intel-db
scripts/psql.sh     # open psql
scripts/down.sh     # docker compose down
```

## Troubleshooting
- If you see a `urllib3` OpenSSL/LibreSSL warning on macOS, it is harmless for this workflow.
- Ensure `.env` is present and `QUICKNODE_BASE_URL` is set to a reachable RPC.
- If schema changes, re-run `make schema`.
